import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AvroGenericRecordSink extends TwoPhaseCommitSinkFunction<GenericRecord, Connection, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroGenericRecordSink.class);
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;

    private static HikariDataSource dataSource;
    private static Map<String, Object> yamlConfig;

    public AvroGenericRecordSink() {
        super(GenericRecord.class, Connection.class, Void.class);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initializeConnectionPool();
        loadYamlConfiguration();
    }

    private void initializeConnectionPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://hostname:port/dbname");
        config.setUsername("username");
        config.setPassword("password");
        config.setMaximumPoolSize(10);
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
    protected Connection beginTransaction() throws Exception {
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    protected void invoke(Connection connection, GenericRecord record, Context context) throws Exception {
        retryOperation(() -> {
            String tableName = record.get("table").toString();

            Map<String, Object> tableDetails = getTableDetails(tableName);
            List<Map<String, String>> fields = getFieldsMapping(tableDetails);

            String[] fieldNames = fields.stream().map(field -> field.get("dbField")).toArray(String[]::new);
            Object[] values = fields.stream().map(field -> convertValue(record.get(field.get("avroField")))).toArray();

            String primaryKeyColumn = (String) tableDetails.get("primaryKeyColumn");
            try (PreparedStatement stmt = prepareUpsertStatement(connection, tableName, fieldNames, values, primaryKeyColumn)) {
                stmt.executeUpdate();
            }
        });
    }

    private Map<String, Object> getTableDetails(String tableName) {
        Map<String, Object> tableConfig = (Map<String, Object>) yamlConfig.get("tables");
        Map<String, Object> tableDetails = (Map<String, Object>) tableConfig.get(tableName);
        if (tableDetails == null) {
            throw new IllegalArgumentException("No configuration found for table: " + tableName);
        }
        return tableDetails;
    }

    private List<Map<String, String>> getFieldsMapping(Map<String, Object> tableDetails) {
        List<Map<String, String>> fields = (List<Map<String, String>>) tableDetails.get("fields");
        if (fields == null) {
            throw new IllegalArgumentException("No field mapping found for table");
        }
        return fields;
    }

    private void retryOperation(Runnable operation) throws Exception {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                operation.run();
                break;
            } catch (SQLException e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    LOG.error("Error processing record after {} attempts", attempt, e);
                    throw new RuntimeException("Error processing record after " + attempt + " attempts", e);
                } else {
                    LOG.warn("Retrying operation (attempt {}/{})", attempt, MAX_RETRIES);
                    Thread.sleep(RETRY_DELAY_MS);
                }
            }
        }
    }

    private PreparedStatement prepareUpsertStatement(Connection connection, String tableName, String[] fieldNames, Object[] values, String primaryKeyColumn) throws SQLException {
        String insertColumns = String.join(", ", fieldNames);
        String insertPlaceholders = IntStream.range(0, fieldNames.length).mapToObj(i -> "?").collect(Collectors.joining(", "));
        String updateClause = IntStream.range(0, fieldNames.length).mapToObj(i -> fieldNames[i] + " = EXCLUDED." + fieldNames[i]).collect(Collectors.joining(", "));
        String query = String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s", tableName, insertColumns, insertPlaceholders, primaryKeyColumn, updateClause);
        PreparedStatement stmt = connection.prepareStatement(query);
        setPreparedStatementValues(stmt, fieldNames, values);
        return stmt;
    }

    private void setPreparedStatementValues(PreparedStatement stmt, String[] fieldNames, Object[] values) throws SQLException {
        for (int i = 0; i < values.length; i++) {
            setValue(stmt, i + 1, fieldNames[i], values[i]);
        }
    }

    private void setValue(PreparedStatement stmt, int index, String dbDatatype, Object value) throws SQLException {
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
    }

    private Object convertValue(Object value) {
        return value;
    }

    @Override
    protected void preCommit(Connection transaction) throws Exception {
    }

    @Override
    protected void commit(Connection transaction) {
        try {
            transaction.commit();
        } catch (SQLException e) {
            LOG.error("Error committing transaction", e);
            throw new RuntimeException("Error committing transaction", e);
        } finally {
            closeConnection(transaction);
        }
    }

    @Override
    protected void abort(Connection transaction) {
        try {
            transaction.rollback();
        } catch (SQLException e) {
            LOG.error("Error rolling back transaction", e);
            throw new RuntimeException("Error rolling back transaction", e);
        } finally {
            closeConnection(transaction);
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
    protected void recoverAndCommit(Connection transaction) throws Exception {
        commit(transaction);
    }

    @Override
    protected void recoverAndAbort(Connection transaction) throws Exception {
        abort(transaction);
    }
}