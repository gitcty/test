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
        // Configure and initialize HikariCP connection pool
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://hostname:port/dbname");
        config.setUsername("username");
        config.setPassword("password");
        config.setMaximumPoolSize(10); // Set pool size according to your requirements
        config.setAutoCommit(false);
        dataSource = new HikariDataSource(config);

        // Load YAML configuration
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
        // Borrow a connection from the pool
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    protected void invoke(Connection connection, GenericRecord record, Context context) throws Exception {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                String operation = record.get("operation").toString();
                String tableName = record.get("table").toString();

                // Extracting field mappings from the YAML configuration
                Map<String, Object> tableConfig = (Map<String, Object>) yamlConfig.get("tables");
                Map<String, Object> tableDetails = (Map<String, Object>) tableConfig.get(tableName);
                List<Map<String, String>> fields = (List<Map<String, String>>) tableDetails.get("fields");
                if (fields == null) {
                    throw new IllegalArgumentException("No field mapping found for table: " + tableName);
                }

                String[] fieldNames = fields.stream().map(field -> field.get("dbField")).toArray(String[]::new);
                Object[] values = fields.stream().map(field -> convertValue(record.get(field.get("avroField")))).toArray();

                try (PreparedStatement stmt = prepareStatement(connection, operation, tableName, tableDetails, fieldNames, values)) {
                    stmt.executeUpdate();
                }

                // If the operation succeeds, break out of the retry loop
                break;
            } catch (SQLException e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    LOG.error("Error processing record after {} attempts: {}", attempt, record, e);
                    throw new RuntimeException("Error processing record after " + attempt + " attempts: " + record, e);
                } else {
                    LOG.warn("Retrying operation for record {} (attempt {}/{})", record, attempt, MAX_RETRIES);
                    Thread.sleep(RETRY_DELAY_MS);
                }
            }
        }
    }

    private PreparedStatement prepareStatement(Connection connection, String operation, String tableName, Map<String, Object> tableDetails, String[] fieldNames, Object[] values) throws SQLException {
        String query;
        switch (operation.toUpperCase()) {
            case "INSERT":
                String insertColumns = String.join(", ", fieldNames);
                String insertPlaceholders = IntStream.range(0, fieldNames.length)
                        .mapToObj(i -> "?")
                        .collect(Collectors.joining(", "));
                query = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, insertColumns, insertPlaceholders);
                PreparedStatement insertStmt = connection.prepareStatement(query);
                setPreparedStatementValues(insertStmt, fields, values);
                return insertStmt;
            case "UPDATE":
                String setClause = IntStream.range(1, fieldNames.length)
                        .mapToObj(i -> fieldNames[i] + " = ?")
                        .collect(Collectors.joining(", "));
                query = String.format("UPDATE %s SET %s WHERE %s = ?", tableName, setClause, fieldNames[0]);
                PreparedStatement updateStmt = connection.prepareStatement(query);
                setPreparedStatementValues(updateStmt, fields, values);
                updateStmt.setObject(values.length, values[0]); // Assuming the first field is the key
                return updateStmt;
            case "DELETE":
                String deleteCondition = (String) tableDetails.get("deleteCondition");
                if (deleteCondition == null || deleteCondition.isEmpty()) {
                    throw new IllegalArgumentException("No delete condition found for table: " + tableName);
                }
                query = String.format("DELETE FROM %s WHERE %s", tableName, deleteCondition);
                PreparedStatement deleteStmt = connection.prepareStatement(query);
                setDeleteConditionValues(deleteStmt, deleteCondition, fields, record);
                return deleteStmt;
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

    private void setDeleteConditionValues(PreparedStatement stmt, String deleteCondition, List<Map<String, String>> fields, GenericRecord record) throws SQLException {
        String[] conditionFields = deleteCondition.split(" AND | OR ");
        int index = 1;
        for (String conditionField : conditionFields) {
            String dbField = conditionField.split("=")[0].trim();
            Map<String, String> fieldMapping = fields.stream().filter(field -> field.get("dbField").equals(dbField)).findFirst().orElse(null);
            if (fieldMapping == null) {
                throw new IllegalArgumentException("No field mapping found for condition field: " + dbField);
            }
            String dbDatatype = fieldMapping.get("dbDatatype");
            Object value = record.get(fieldMapping.get("avroField"));
            setValue(stmt, index++, dbDatatype, value);
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
                stmt.setObject(index, value); // Fallback for other types
                break;
        }
    }

    private Object convertValue(Object value) {
        // Add conversion logic if needed, currently returning value directly
        return value;
    }

    @Override
    protected void preCommit(Connection transaction) throws Exception {
        // Nothing to do here, individual statements are executed during invoke
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
        // Close the data source when the sink is closed
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