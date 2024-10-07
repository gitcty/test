import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateInitializationContext;
import org.apache.flink.api.common.state.StateSnapshotContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroUpsertSink extends RichSinkFunction<GenericRecord> implements CheckpointedFunction {

    private transient ListState<Map<String, GenericRecord>> checkpointedState;
    private Map<String, GenericRecord> bufferedRecords;

    private String tableName;
    private String topicName; // Store the topic name
    private List<ColumnMapping> columnMappings;
    private List<DeleteCondition> deleteConditions;
    private String combineWith; // "AND" or "OR"

    private String jdbcUrl;
    private String username;
    private String password;
    private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // Load YAML configuration
        Yaml yaml = new Yaml();
        try (InputStream input = getClass().getResourceAsStream("/schema-config.yaml")) {
            Map<String, Object> config = yaml.load(input);
            topicName = (String) config.get("topic_name"); // Read the topic name
            tableName = (String) ((Map<String, Object>) config.get("table")).get("name");

            List<Map<String, String>> columns = (List<Map<String, String>>) ((Map<String, Object>) config.get("table")).get("columns");
            columnMappings = new ArrayList<>();
            for (Map<String, String> column : columns) {
                columnMappings.add(new ColumnMapping(column.get("avro_field"), column.get("sql_column"), column.get("type")));
            }

            // Load delete condition
            Map<String, Object> deleteConditionConfig = (Map<String, Object>) ((Map<String, Object>) config.get("table")).get("delete_condition");
            List<Map<String, String>> conditions = (List<Map<String, String>>) deleteConditionConfig.get("conditions");
            combineWith = (String) deleteConditionConfig.get("combine_with");

            deleteConditions = new ArrayList<>();
            for (Map<String, String> condition : conditions) {
                deleteConditions.add(new DeleteCondition(condition.get("column"), condition.get("avro_field"), condition.get("operator")));
            }

            // Database connection parameters
            Map<String, String> dbConfig = (Map<String, String>) config.get("database");
            jdbcUrl = dbConfig.get("jdbcUrl");
            username = dbConfig.get("username");
            password = dbConfig.get("password");
        }

        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);

        bufferedRecords = new HashMap<>();
    }

    @Override
    public void invoke(GenericRecord record, Context context) throws Exception {
        // Extract the operation from <topicName>headers
        GenericRecord headers = (GenericRecord) record.get(topicName + "headers");
        String operation = headers.get("operation").toString();

        switch (operation.toUpperCase()) {
            case "INSERT":
            case "UPDATE":
                upsertRecordToDB(record);
                break;
            case "DELETE":
                deleteRecordFromDB(record);
                break;
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

    private void upsertRecordToDB(GenericRecord record) throws SQLException {
        // Extract the nested data record using the topic name
        GenericRecord data = (GenericRecord) record.get(topicName + "data");

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (");

        for (ColumnMapping mapping : columnMappings) {
            sql.append(mapping.getSqlColumn()).append(",");
        }
        sql.deleteCharAt(sql.length() - 1).append(") VALUES (");

        for (int i = 0; i < columnMappings.size(); i++) {
            sql.append("?,");
        }
        sql.deleteCharAt(sql.length() - 1).append(") ON CONFLICT DO UPDATE SET ");

        for (ColumnMapping mapping : columnMappings) {
            sql.append(mapping.getSqlColumn()).append(" = EXCLUDED.").append(mapping.getSqlColumn()).append(",");
        }
        sql.deleteCharAt(sql.length() - 1);

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (ColumnMapping mapping : columnMappings) {
                // Extract the field value from the nested structure
                String[] fieldPath = mapping.getAvroField().split("\\.");
                Object fieldValue = extractFieldValue(data, fieldPath);
                setPreparedStatementValue(statement, index++, fieldValue, mapping.getType());
            }
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    private void deleteRecordFromDB(GenericRecord record) throws SQLException {
        // Extract the nested data record using the topic name
        GenericRecord data = (GenericRecord) record.get(topicName + "data");

        // Build the DELETE SQL statement using the conditions in the YAML configuration
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(tableName).append(" WHERE ");

        for (int i = 0; i < deleteConditions.size(); i++) {
            DeleteCondition condition = deleteConditions.get(i);
            sql.append(condition.getColumn()).append(" ").append(condition.getOperator()).append(" ?");
            if (i < deleteConditions.size() - 1) {
                sql.append(" ").append(combineWith).append(" ");
            }
        }

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (DeleteCondition condition : deleteConditions) {
                // Extract the field value from the nested structure
                String[] fieldPath = condition.getAvroField().split("\\.");
                Object fieldValue = extractFieldValue(data, fieldPath);
                setPreparedStatementValue(statement, index++, fieldValue, condition.getType());
            }
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    private Object extractFieldValue(GenericRecord record, String[] fieldPath) {
        Object current = record;
        for (String field : fieldPath) {
            if (current instanceof GenericRecord) {
                current = ((GenericRecord) current).get(field);
            } else {
                return null;
            }
        }
        return current;
    }

    private void setPreparedStatementValue(PreparedStatement statement, int index, Object value, String type) throws SQLException {
        if (value == null) {
            statement.setNull(index, getSQLType(type));
            return;
        }

        switch (type.toUpperCase()) {
            case "VARCHAR":
            case "TEXT":
                statement.setString(index, value.toString());
                break;
            case "INTEGER":
                statement.setInt(index, Integer.parseInt(value.toString()));
                break;
            case "BIGINT":
                statement.setLong(index, Long.parseLong(value.toString()));
                break;
            case "DOUBLE":
                statement.setDouble(index, Double.parseDouble(value.toString()));
                break;
            case "BOOLEAN":
                statement.setBoolean(index, Boolean.parseBoolean(value.toString()));
                break;
            case "DATE":
                statement.setDate(index, Date.valueOf(value.toString()));
                break;
            case "TIMESTAMP":
                statement.setTimestamp(index, Timestamp.valueOf(value.toString()));
                break;
            case "UUID":
                statement.setObject(index, java.util.UUID.fromString(value.toString()), Types.OTHER);
                break;
            case "JSON":
            case "JSONB":
                statement.setObject(index, value.toString(), Types.OTHER);
                break;
            case "ARRAY":
                if (value instanceof List) {
                    List<?> list = (List<?>) value;
                    Array array = connection.createArrayOf("TEXT", list.toArray()); // Adjust the array type as needed
                    statement.setArray(index, array);
                } else {
                    throw new IllegalArgumentException("Unsupported array type for value: " + value);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported SQL type: " + type);
        }
    }

    private int getSQLType(String type) {
        switch (type.toUpperCase()) {
            case "VARCHAR":
            case "TEXT":
                return Types.VARCHAR;
            case "INTEGER":
                return Types.INTEGER;
            case "BIGINT":
                return Types.BIGINT;
            case "DOUBLE":
                return Types.DOUBLE;
            case "BOOLEAN":
                return Types.BOOLEAN;
            case "DATE":
                return Types.DATE;
            case "TIMESTAMP":
                return Types.TIMESTAMP;
            case "UUID":
                return Types.OTHER;
            case "JSON":
            case "JSONB":
                return Types.OTHER;
            case "ARRAY":
                return Types.ARRAY;
            default:
                throw new IllegalArgumentException("Unsupported SQL type: " + type);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        checkpointedState.clear();
        checkpointedState.add(new HashMap<>(bufferedRecords));
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        ListStateDescriptor<Map<String, GenericRecord>> descriptor = new ListStateDescriptor<>(
                "buffered-records",
                TypeInformation.of(Map.class)
        );

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Map<String, GenericRecord> state : checkpointedState.get()) {
                bufferedRecords.putAll(state);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    // Helper class to store column mapping information
    private static class ColumnMapping {
        private final String avroField;
        private final String sqlColumn;
        private final String type;

        public ColumnMapping(String avroField, String sqlColumn, String type) {
            this.avroField = avroField;
            this.sqlColumn = sqlColumn;
            this.type = type;
        }

        public String getAvroField() {
            return avroField;
        }

        public String getSqlColumn() {
            return sqlColumn;
        }

        public String getType() {
            return type;
        }
    }

    // Helper class for delete conditions
    private static class DeleteCondition {
        private final String column;
        private final String avroField;
        private final String operator;
        private final String type;

        public DeleteCondition(String column, String avroField, String operator) {
            this.column = column;
            this.avroField = avroField;
            this.operator = operator;
            this.type = "VARCHAR"; // Default type, modify as needed
        }

        public String getColumn() {
            return column;
        }

        public String getAvroField() {
            return avroField;
        }

        public String getOperator() {
            return operator;
        }

        public String getType() {
            return type;
        }
    }
}
