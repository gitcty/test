import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.function.Function;
import java.util.function.Supplier;

public class AvroGenericRecordSink extends TwoPhaseCommitSinkFunction<GenericRecord, ConnectionState, Void> implements CheckpointListener {
    private static final Logger LOG = LoggerFactory.getLogger(AvroGenericRecordSink.class);
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;

    private static HikariDataSource dataSource;
    private static Map<String, Object> yamlConfig;

    public AvroGenericRecordSink() {
        super(new ConnectionStateSerializer(), VoidSerializer.INSTANCE);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        loadYamlConfiguration();
        initializeConnectionPool();
    }

    private void initializeConnectionPool() {
        HikariConfig config = new HikariConfig();
        Map<String, String> dbConfig = (Map<String, String>) yamlConfig.get("database");

        if (dbConfig == null) {
            throw new IllegalArgumentException("Database configuration is missing in the YAML file.");
        }

        String jdbcUrl = dbConfig.get("jdbcUrl");
        String username = dbConfig.get("username");
        String password = dbConfig.get("password");
        int maxPoolSize = Integer.parseInt(dbConfig.getOrDefault("maxPoolSize", "10"));

        if (jdbcUrl == null || username == null || password == null) {
            throw new IllegalArgumentException("JDBC URL, username, or password is missing in the database configuration.");
        }

        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maxPoolSize);
        config.setAutoCommit(false);

        dataSource = new HikariDataSource(config);
    }

    private void loadYamlConfiguration() {
        try (InputStream input = new FileInputStream("config.yaml")) {
            Yaml yaml = new Yaml();
            yamlConfig = yaml.load(input);
        } catch (IOException e) {
            LOG.error("Error loading YAML configuration file", e);
            throw new RuntimeException("Error loading YAML configuration file", e);
        }
    }

    @Override
    protected ConnectionState beginTransaction() throws Exception {
        return getConnection(() -> {
            String jdbcUrl = dataSource.getJdbcUrl();
            String username = dataSource.getUsername();
            String password = dataSource.getPassword();
            return new ConnectionState(jdbcUrl, username, password);
        });
    }

    @Override
    protected void invoke(ConnectionState transaction, GenericRecord record, Context context) throws Exception {
        retryOperation(() -> processRecord(transaction.getConnection(), record));
    }

    private void processRecord(Connection connection, GenericRecord record) throws SQLException {
        String operation = record.get("operation").toString().toUpperCase();
        String tableName = record.get("tableName").toString();
        Map<String, Object> tableDetails = getTableDetails(tableName);
        List<Map<String, String>> fields = getFieldsMapping(tableDetails);

        String[] fieldNames = fields.stream().map(field -> field.get("dbField")).toArray(String[]::new);
        Object[] values = fields.stream().map(field -> convertValue(record.get(field.get("avroField")))).toArray();

        try (PreparedStatement stmt = ("DELETE".equals(operation)) ?
                prepareGenericDeleteStatement(connection, tableName, fieldNames, values) :
                prepareUpsertStatement(connection, tableName, fieldNames, values, (String) tableDetails.get("primaryKeyColumn"))) {
            stmt.executeUpdate();
        }
    }

    private Map<String, Object> getTableDetails(String tableName) {
        return getConfigMap("tables", tableName, "No configuration found for table: " + tableName);
    }

    private List<Map<String, String>> getFieldsMapping(Map<String, Object> tableDetails) {
        return getConfigList(tableDetails, "fields", "No field mapping found for table");
    }

    private void retryOperation(Runnable operation) throws Exception {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                operation.run();
                break;
            } catch (SQLException e) {
                if (attempt >= MAX_RETRIES - 1) {
                    LOG.error("Error processing record after {} attempts", attempt + 1, e);
                    throw new RuntimeException("Error processing record after " + (attempt + 1) + " attempts", e);
                } else {
                    LOG.warn("Retrying operation (attempt {}/{})", attempt + 1, MAX_RETRIES);
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private PreparedStatement prepareUpsertStatement(Connection connection, String tableName, String[] fieldNames, Object[] values, String primaryKeyColumn) throws SQLException {
        String insertColumns = String.join(", ", fieldNames);
        String insertPlaceholders = IntStream.range(0, fieldNames.length).mapToObj(i -> "?").collect(Collectors.joining(", "));
        String updateClause = IntStream.range(0, fieldNames.length).mapToObj(i -> fieldNames[i] + " = EXCLUDED." + fieldNames[i]).collect(Collectors.joining(", "));
        String query = String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s", tableName, insertColumns, insertPlaceholders, primaryKeyColumn, updateClause);
        return setPreparedStatementValues(connection.prepareStatement(query), fieldNames, values);
    }

    private PreparedStatement prepareGenericDeleteStatement(Connection connection, String tableName, String[] fieldNames, Object[] values) throws SQLException {
        String deleteConditions = IntStream.range(0, fieldNames.length)
                .mapToObj(i -> fieldNames[i] + " = ?")
                .collect(Collectors.joining(" AND "));
        String query = String.format("DELETE FROM %s WHERE %s", tableName, deleteConditions);
        PreparedStatement stmt = connection.prepareStatement(query);
        for (int i = 0; i < values.length; i++) {
            stmt.setObject(i + 1, values[i]);
        }
        return stmt;
    }

    private PreparedStatement setPreparedStatementValues(PreparedStatement stmt, String[] fieldNames, Object[] values) throws SQLException {
        for (int i = 0; i < values.length; i++) {
            setValue(stmt, i + 1, fieldNames[i], values[i]);
        }
        return stmt;
    }

    private void setValue(PreparedStatement stmt, int index, String dbDatatype, Object value) {
        try {
            switch (dbDatatype.toLowerCase()) {
                case "string":
                    stmt.setString(index, (String) value);
                    break;
                case "int":
                case "integer":
                    stmt.setInt(index, (Integer) value);
                    break;
                case "long":
                    stmt.setLong(index, (Long) value);
                    break;
                case "double":
                    stmt.setDouble(index, (Double) value);
                    break;
                case "float":
                    stmt.setFloat(index, (Float) value);
                    break;
                case "boolean":
                    stmt.setBoolean(index, (Boolean) value);
                    break;
                case "date":
                    stmt.setDate(index, (java.sql.Date) value);
                    break;
                case "timestamp":
                    stmt.setTimestamp(index, (java.sql.Timestamp) value);
                    break;
                default:
                    stmt.setObject(index, value);
                    break;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error setting value for index " + index, e);
        }
    }

    private Object convertValue(Object value) {
        return value;
    }

    private <T> T getConfigValue(Supplier<T> supplier, String errorMessage) {
        T result = supplier.get();
        if (result == null) {
            throw new IllegalArgumentException(errorMessage);
        }
        return result;
    }

    private Map<String, Object> getConfigMap(String parentKey, String childKey, String errorMessage) {
        return getConfigValue(() -> (Map<String, Object>) ((Map<String, Object>) yamlConfig.get(parentKey)).get(childKey), errorMessage);
    }

    private List<Map<String, String>> getConfigList(Map<String, Object> parentMap, String childKey, String errorMessage) {
        return getConfigValue(() -> (List<Map<String, String>>) parentMap.get(childKey), errorMessage);
    }

    private ConnectionState getConnection(Supplier<ConnectionState> connectionSupplier) {
        return getConfigValue(connectionSupplier, "Error obtaining a connection from the pool");
    }

    @Override
    protected void preCommit(ConnectionState transaction) throws Exception {}

    @Override
    protected void commit(ConnectionState transaction) {
        try {
            transaction.getConnection().commit();
        } catch (SQLException e) {
            LOG.error("Error committing transaction", e);
            throw new RuntimeException("Error committing transaction", e);
        } finally {
            closeConnection(transaction.getConnection());
        }
    }

    @Override
    protected void abort(ConnectionState transaction) {
        try {
            transaction.getConnection().rollback();
        } catch (SQLException e) {
            LOG.error("Error rolling back transaction", e);
            throw new RuntimeException("Error rolling back transaction", e);
        } finally {
            closeConnection(transaction.getConnection());
        }
    }

    private void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("Error closing connection", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
        super.close();
    }

    @Override
    protected void recoverAndCommit(ConnectionState transaction) throws Exception {
        commit(transaction);
    }

    @Override
    protected void recoverAndAbort(ConnectionState transaction) throws Exception {
        abort(transaction);
    }
}

class ConnectionState {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public ConnectionState(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public Connection getConnection() throws SQLException {
        Connection connection = java.sql.DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);
        return connection;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}

class ConnectionStateSerializer extends TypeSerializer<ConnectionState> {
    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<ConnectionState> duplicate() {
        return this;
    }

    @Override
    public ConnectionState createInstance() {
        return null;
    }

    @Override
    public ConnectionState copy(ConnectionState from) {
        return new ConnectionState(from.getJdbcUrl(), from.getUsername(), from.getPassword());
    }

    @Override
    public ConnectionState copy(ConnectionState from, ConnectionState reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ConnectionState record, java.io.DataOutputView target) throws IOException {
        target.writeUTF(record.getJdbcUrl());
        target.writeUTF(record.getUsername());
        target.writeUTF(record.getPassword());
    }

    @Override
    public ConnectionState deserialize(java.io.DataInputView source) throws IOException {
        String jdbcUrl = source.readUTF();
        String username = source.readUTF();
        String password = source.readUTF();
        return new ConnectionState(jdbcUrl, username, password);
    }

    @Override
    public ConnectionState deserialize(ConnectionState reuse, java.io.DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(java.io.DataInputView source, java.io.DataOutputView target) throws IOException {
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
    }

    @Override
    public TypeSerializerSnapshot<ConnectionState> snapshotConfiguration() {
        return new ConnectionStateSerializerSnapshot();
    }

    public static class ConnectionStateSerializerSnapshot extends TypeSerializerSnapshot<ConnectionState> {
        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(java.io.DataOutputView out) throws IOException {
        }

        @Override
        public void readSnapshot(int readVersion, java.io.DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        }

        @Override
        public TypeSerializer<ConnectionState> restoreSerializer() {
            return new ConnectionStateSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<ConnectionState> resolveSchemaCompatibility(TypeSerializer<ConnectionState> newSerializer) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
    }
}
