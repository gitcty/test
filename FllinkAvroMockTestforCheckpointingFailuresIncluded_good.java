import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class AvroGenericRecordSinkTest {

    private Server h2Server;
    private static final String CONFIG_FILE = "config.yaml";
    private static final Random RANDOM = new Random();

    @Before
    public void setUp() throws Exception {
        // Start H2 server for mock DB
        h2Server = Server.createTcpServer("-tcpPort", "9092", "-tcpAllowOthers").start();

        // Create a mock config file for test mode
        Map<String, Object> config = new HashMap<>();
        Map<String, String> databaseConfig = new HashMap<>();
        databaseConfig.put("jdbcUrl", "jdbc:h2:tcp://localhost:9092/mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        databaseConfig.put("username", "sa");
        databaseConfig.put("password", "");
        databaseConfig.put("maxPoolSize", "5");
        config.put("database", databaseConfig);

        Yaml yaml = new Yaml();
        try (FileWriter writer = new FileWriter(CONFIG_FILE)) {
            yaml.dump(config, writer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write test configuration file", e);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (h2Server != null) {
            h2Server.stop();
        }
    }

    @Test
    public void testAvroGenericRecordSink() throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing with a low interval to test checkpointing under stress
        env.enableCheckpointing(1000); // 1-second interval
        env.getCheckpointConfig().setCheckpointTimeout(2000); // 2-second timeout to introduce checkpoint pressure

        // Create a source of sample Avro records
        DataStream<GenericRecord> sourceStream = env.addSource(new SourceFunction<GenericRecord>() {
            private boolean running = true;

            @Override
            public void run(SourceContext<GenericRecord> ctx) throws Exception {
                // Sample schema definition
                Schema schema = new Schema.Parser().parse(
                        "{\"type\": \"record\", \"name\": \"EmployeeRecord\", " +
                                "\"fields\": [" +
                                "{\"name\": \"tableName\", \"type\": \"string\"}," +
                                "{\"name\": \"employeeId\", \"type\": \"int\"}," +
                                "{\"name\": \"firstName\", \"type\": \"string\"}," +
                                "{\"name\": \"lastName\", \"type\": \"string\"}," +
                                "{\"name\": \"department\", \"type\": \"string\"}," +
                                "{\"name\": \"salary\", \"type\": \"double\"}," +
                                "{\"name\": \"hireDate\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"}," +
                                "{\"name\": \"isActive\", \"type\": \"boolean\"}]"
                );

                int idCounter = 1;
                while (running && idCounter <= 10) {
                    // Insert a record
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("tableName", "employee");
                    record.put("employeeId", idCounter);
                    record.put("firstName", "FirstName_" + idCounter);
                    record.put("lastName", "LastName_" + idCounter);
                    record.put("department", "Department_" + idCounter);
                    record.put("salary", 50000.0 + (idCounter * 1000));
                    record.put("hireDate", System.currentTimeMillis());
                    record.put("isActive", idCounter % 2 == 0);
                    ctx.collect(record);

                    // Introduce a failure for testing transaction and checkpoint recovery
                    if (RANDOM.nextInt(10) < 3) { // 30% chance of failure
                        throw new RuntimeException("Simulated transaction failure for testing.");
                    }

                    Thread.sleep(500); // Emit a record every 500ms
                    idCounter++;
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        // Add a sink that processes the GenericRecord using AvroGenericRecordSink
        sourceStream.addSink(new AvroGenericRecordSink() {
            @Override
            protected void preCommit(Connection transaction) throws Exception {
                // Introduce failure to test checkpointing failure scenario
                if (RANDOM.nextInt(10) < 2) { // 20% chance to fail during pre-commit
                    throw new RuntimeException("Simulated checkpoint pre-commit failure for testing.");
                }
                super.preCommit(transaction);
            }

            @Override
            protected void commit(Connection transaction) {
                // Introduce failure to test checkpoint commit failure scenario
                if (RANDOM.nextInt(10) < 2) { // 20% chance to fail during commit
                    throw new RuntimeException("Simulated checkpoint commit failure for testing.");
                }
                super.commit(transaction);
            }
        });

        // Execute the Flink job
        env.execute("Flink Avro GenericRecord Sink Test");
    }
}
