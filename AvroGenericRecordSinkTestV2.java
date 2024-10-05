import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class AvroGenericRecordSink extends RichSinkFunction<GenericRecord> implements CheckpointedFunction, CheckpointListener {
    private Connection connection;
    private PreparedStatement insertStmt;
    private PreparedStatement updateStmt;
    private PreparedStatement deleteStmt;
    private final Map<Long, Runnable> checkpointCallbacks = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Establish database connection
        String url = "jdbc:mysql://hostname:port/dbname";
        String username = "username";
        String password = "password";
        connection = DriverManager.getConnection(url, username, password);

        // Prepare SQL statements
        insertStmt = connection.prepareStatement("INSERT INTO my_table (key_col, value_col) VALUES (?, ?)");
        updateStmt = connection.prepareStatement("UPDATE my_table SET value_col = ? WHERE key_col = ?");
        deleteStmt = connection.prepareStatement("DELETE FROM my_table WHERE key_col = ?");
    }

    @Override
    public void invoke(GenericRecord record, Context context) throws Exception {
        String operation = record.get("operation").toString();
        String key = record.get("key").toString();
        String value = record.get("value") != null ? record.get("value").toString() : null;

        switch (operation.toUpperCase()) {
            case "INSERT":
                insertRecord(key, value);
                break;
            case "UPDATE":
                updateRecord(key, value);
                break;
            case "DELETE":
                deleteRecord(key);
                break;
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

    private void insertRecord(String key, String value) throws SQLException {
        insertStmt.setString(1, key);
        insertStmt.setString(2, value);
        insertStmt.addBatch();
    }

    private void updateRecord(String key, String value) throws SQLException {
        updateStmt.setString(1, value);
        updateStmt.setString(2, key);
        updateStmt.addBatch();
    }

    private void deleteRecord(String key) throws SQLException {
        deleteStmt.setString(1, key);
        deleteStmt.addBatch();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Register callback to commit batches upon checkpoint completion
        long checkpointId = context.getCheckpointId();
        checkpointCallbacks.put(checkpointId, this::commitBatches);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Initialization logic if needed
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Execute the registered callback for committing on checkpoint completion
        Runnable callback = checkpointCallbacks.remove(checkpointId);
        if (callback != null) {
            callback.run();
        }
    }

    private void commitBatches() {
        try {
            if (insertStmt != null) insertStmt.executeBatch();
            if (updateStmt != null) updateStmt.executeBatch();
            if (deleteStmt != null) deleteStmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Error committing batches", e);
        }
    }

    @Override
    public void close() throws Exception {
        // Close resources
        if (insertStmt != null) insertStmt.close();
        if (updateStmt != null) updateStmt.close();
        if (deleteStmt != null) deleteStmt.close();
        if (connection != null) connection.close();
        super.close();
    }
}

// Flink Job Integration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;

public class FlinkAvroJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing
        env.enableCheckpointing(10000); // checkpoint every 10 seconds

        // Define Avro schema
        String schemaString = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"MyRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"key\", \"type\": \"string\"},\n" +
                "    {\"name\": \"value\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"operation\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaString);

        // Create a sample data stream of GenericRecord
        DataStream<GenericRecord> stream = env.fromElements(
                createGenericRecord(schema, "key1", "value1", "INSERT"),
                createGenericRecord(schema, "key2", "value2", "UPDATE"),
                createGenericRecord(schema, "key3", null, "DELETE")
        );

        // Add the custom sink
        stream.addSink(new AvroGenericRecordSink());

        // Execute the job
        env.execute("Flink Avro GenericRecord Sink Example");
    }

    private static GenericRecord createGenericRecord(Schema schema, String key, String value, String operation) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("key", key);
        record.put("value", value);
        record.put("operation", operation);
        return record;
    }
}

// JUnit Test for AvroGenericRecordSink
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import static org.mockito.Mockito.*;

public class AvroGenericRecordSinkTest {
    private AvroGenericRecordSink sink;
    private Connection mockConnection;
    private PreparedStatement mockInsertStmt;
    private PreparedStatement mockUpdateStmt;
    private PreparedStatement mockDeleteStmt;

    @Before
    public void setUp() throws Exception {
        sink = new AvroGenericRecordSink();
        mockConnection = mock(Connection.class);
        mockInsertStmt = mock(PreparedStatement.class);
        mockUpdateStmt = mock(PreparedStatement.class);
        mockDeleteStmt = mock(PreparedStatement.class);

        // Mock database connection and prepared statements
        when(mockConnection.prepareStatement("INSERT INTO my_table (key_col, value_col) VALUES (?, ?)")).thenReturn(mockInsertStmt);
        when(mockConnection.prepareStatement("UPDATE my_table SET value_col = ? WHERE key_col = ?")).thenReturn(mockUpdateStmt);
        when(mockConnection.prepareStatement("DELETE FROM my_table WHERE key_col = ?")).thenReturn(mockDeleteStmt);

        // Inject mocks into the sink
        sink.connection = mockConnection;
        sink.insertStmt = mockInsertStmt;
        sink.updateStmt = mockUpdateStmt;
        sink.deleteStmt = mockDeleteStmt;
    }

    @Test
    public void testInsertRecord() throws Exception {
        Schema schema = new Schema.Parser().parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"MyRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"key\", \"type\": \"string\"},\n" +
                "    {\"name\": \"value\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"operation\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}");
        GenericRecord record = new GenericData.Record(schema);
        record.put("key", "key1");
        record.put("value", "value1");
        record.put("operation", "INSERT");

        sink.invoke(record, null);

        verify(mockInsertStmt, times(1)).setString(1, "key1");
        verify(mockInsertStmt, times(1)).setString(2, "value1");
        verify(mockInsertStmt, times(1)).addBatch();
    }

    @Test
    public void testUpdateRecord() throws Exception {
        Schema schema = new Schema.Parser().parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"MyRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"key\", \"type\": \"string\"},\n" +
                "    {\"name\": \"value\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"operation\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}");
        GenericRecord record = new GenericData.Record(schema);
        record.put("key", "key2");
        record.put("value", "value2");
        record.put("operation", "UPDATE");

        sink.invoke(record, null);

        verify(mockUpdateStmt, times(1)).setString(1, "value2");
        verify(mockUpdateStmt, times(1)).setString(2, "key2");
        verify(mockUpdateStmt, times(1)).addBatch();
    }

    @Test
    public void testDeleteRecord() throws Exception {
        Schema schema = new Schema.Parser().parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"MyRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"key\", \"type\": \"string\"},\n" +
                "    {\"name\": \"value\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"operation\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}");
        GenericRecord record = new GenericData.Record(schema);
        record.put("key", "key3");
        record.put("operation", "DELETE");

        sink.invoke(record, null);

        verify(mockDeleteStmt, times(1)).setString(1, "key3");
        verify(mockDeleteStmt, times(1)).addBatch();
    }
}
