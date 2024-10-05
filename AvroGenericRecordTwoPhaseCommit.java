import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AvroGenericRecordSink extends TwoPhaseCommitSinkFunction<GenericRecord, Connection, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroGenericRecordSink.class);
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;

    public AvroGenericRecordSink() {
        super(GenericRecord.class, Connection.class, Void.class);
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        // Establish database connection
        String url = "jdbc:mysql://hostname:port/dbname";
        String username = "username";
        String password = "password";
        return DriverManager.getConnection(url, username, password);
    }

    @Override
    protected void invoke(Connection connection, GenericRecord record, Context context) throws Exception {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                String operation = record.get("operation").toString();
                String key = record.get("key").toString();
                String value = record.get("value") != null ? record.get("value").toString() : null;

                switch (operation.toUpperCase()) {
                    case "INSERT":
                        insertRecord(connection, key, value);
                        break;
                    case "UPDATE":
                        updateRecord(connection, key, value);
                        break;
                    case "DELETE":
                        deleteRecord(connection, key);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown operation: " + operation);
                }
                // If the operation succeeds, break out of the retry loop
                break;
            } catch (SQLException e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    // Log the error and rethrow it to fail the task after exhausting retries
                    LOG.error("Error processing record after {} attempts: {}", attempt, record, e);
                    throw new RuntimeException("Error processing record after " + attempt + " attempts: " + record, e);
                } else {
                    // Log the retry attempt
                    LOG.warn("Retrying operation for record {} (attempt {}/{})", record, attempt, MAX_RETRIES);
                    Thread.sleep(RETRY_DELAY_MS);
                }
            }
        }
    }

    private void insertRecord(Connection connection, String key, String value) throws SQLException {
        try (PreparedStatement insertStmt = connection.prepareStatement("INSERT INTO my_table (key_col, value_col) VALUES (?, ?)")) {
            insertStmt.setString(1, key);
            insertStmt.setString(2, value);
            insertStmt.executeUpdate();
        }
    }

    private void updateRecord(Connection connection, String key, String value) throws SQLException {
        try (PreparedStatement updateStmt = connection.prepareStatement("UPDATE my_table SET value_col = ? WHERE key_col = ?")) {
            updateStmt.setString(1, value);
            updateStmt.setString(2, key);
            updateStmt.executeUpdate();
        }
    }

    private void deleteRecord(Connection connection, String key) throws SQLException {
        try (PreparedStatement deleteStmt = connection.prepareStatement("DELETE FROM my_table WHERE key_col = ?")) {
            deleteStmt.setString(1, key);
            deleteStmt.executeUpdate();
        }
    }

    @Override
    protected void preCommit(Connection transaction) throws Exception {
        // No-op for this use case
    }

    @Override
    protected void commit(Connection transaction) {
        try {
            transaction.commit();
        } catch (SQLException e) {
            LOG.error("Error committing transaction", e);
            throw new RuntimeException("Error committing transaction", e);
        }
    }

    @Override
    protected void abort(Connection transaction) {
        try {
            transaction.rollback();
        } catch (SQLException e) {
            LOG.error("Error rolling back transaction", e);
            throw new RuntimeException("Error rolling back transaction", e);
        }
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import static org.mockito.Mockito.*;

public class AvroGenericRecordSinkTest {
    private static final Logger LOG = LoggerFactory.getLogger(AvroGenericRecordSinkTest.class);
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

        sink.invoke(mockConnection, record, null);

        verify(mockInsertStmt, times(1)).setString(1, "key1");
        verify(mockInsertStmt, times(1)).setString(2, "value1");
        verify(mockInsertStmt, times(1)).executeUpdate();
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
        Generic