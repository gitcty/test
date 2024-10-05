import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkAvroJob {
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a source of sample Avro records
        DataStream<GenericRecord> sourceStream = env.addSource(new SourceFunction<GenericRecord>() {
            private boolean running = true;

            @Override
            public void run(SourceContext<GenericRecord> ctx) throws Exception {
                // Sample schema definition
                org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(
                        "{\"type\": \"record\", \"name\": \"EmployeeRecord\", " +
                        "\"fields\": [" +
                        "{\"name\": \"employeeId\", \"type\": \"int\"}," +
                        "{\"name\": \"firstName\", \"type\": \"string\"}," +
                        "{\"name\": \"lastName\", \"type\": \"string\"}," +
                        "{\"name\": \"department\", \"type\": \"string\"}," +
                        "{\"name\": \"salary\", \"type\": \"double\"}," +
                        "{\"name\": \"hireDate\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"}," +
                        "{\"name\": \"isActive\", \"type\": \"boolean\"}]"
                );

                int idCounter = 1;
                while (running) {
                    // Insert a record
                    GenericRecord insertRecord = new GenericData.Record(schema);
                    insertRecord.put("employeeId", idCounter);
                    insertRecord.put("firstName", "FirstName_" + idCounter);
                    insertRecord.put("lastName", "LastName_" + idCounter);
                    insertRecord.put("department", "Department_" + idCounter);
                    insertRecord.put("salary", 50000.0 + (idCounter * 1000));
                    insertRecord.put("hireDate", System.currentTimeMillis());
                    insertRecord.put("isActive", idCounter % 2 == 0);
                    ctx.collect(insertRecord);

                    Thread.sleep(1000); // Emit an insert record

                    // Upsert the same record with modified value
                    GenericRecord upsertRecord = new GenericData.Record(schema);
                    upsertRecord.put("employeeId", idCounter);
                    upsertRecord.put("firstName", "UpdatedFirstName_" + idCounter);
                    upsertRecord.put("lastName", "UpdatedLastName_" + idCounter);
                    upsertRecord.put("department", "UpdatedDepartment_" + idCounter);
                    upsertRecord.put("salary", 60000.0 + (idCounter * 1000));
                    upsertRecord.put("hireDate", System.currentTimeMillis());
                    upsertRecord.put("isActive", idCounter % 2 != 0);
                    ctx.collect(upsertRecord);

                    Thread.sleep(1000); // Emit an upsert record

                    // Insert a record with a different id to simulate a new entry
                    GenericRecord newInsertRecord = new GenericData.Record(schema);
                    newInsertRecord.put("employeeId", idCounter + 1);
                    newInsertRecord.put("firstName", "FirstName_" + (idCounter + 1));
                    newInsertRecord.put("lastName", "LastName_" + (idCounter + 1));
                    newInsertRecord.put("department", "Department_" + (idCounter + 1));
                    newInsertRecord.put("salary", 55000.0 + (idCounter * 1000));
                    newInsertRecord.put("hireDate", System.currentTimeMillis());
                    newInsertRecord.put("isActive", (idCounter + 1) % 2 == 0);
                    ctx.collect(newInsertRecord);

                    Thread.sleep(1000); // Emit a new insert record

                    // Upsert the record with the same id to test the conflict handling
                    GenericRecord conflictUpsertRecord = new GenericData.Record(schema);
                    conflictUpsertRecord.put("employeeId", idCounter);
                    conflictUpsertRecord.put("firstName", "ConflictFirstName_" + idCounter);
                    conflictUpsertRecord.put("lastName", "ConflictLastName_" + idCounter);
                    conflictUpsertRecord.put("department", "ConflictDepartment_" + idCounter);
                    conflictUpsertRecord.put("salary", 70000.0 + (idCounter * 1000));
                    conflictUpsertRecord.put("hireDate", System.currentTimeMillis());
                    conflictUpsertRecord.put("isActive", idCounter % 2 == 0);
                    ctx.collect(conflictUpsertRecord);

                    Thread.sleep(1000); // Emit a conflict upsert record

                    // Delete the record
                    GenericRecord deleteRecord = new GenericData.Record(schema);
                    deleteRecord.put("employeeId", idCounter);
                    deleteRecord.put("firstName", "");
                    deleteRecord.put("lastName", "");
                    deleteRecord.put("department", "");
                    deleteRecord.put("salary", 0.0);
                    deleteRecord.put("hireDate", 0L);
                    deleteRecord.put("isActive", false);
                    ctx.collect(deleteRecord);

                    Thread.sleep(1000); // Emit a delete record

                    // Delete the newly inserted record
                    GenericRecord newDeleteRecord = new GenericData.Record(schema);
                    newDeleteRecord.put("employeeId", idCounter + 1);
                    newDeleteRecord.put("firstName", "");
                    newDeleteRecord.put("lastName", "");
                    newDeleteRecord.put("department", "");
                    newDeleteRecord.put("salary", 0.0);
                    newDeleteRecord.put("hireDate", 0L);
                    newDeleteRecord.put("isActive", false);
                    ctx.collect(newDeleteRecord);

                    Thread.sleep(1000); // Emit a delete record for the new entry

                    idCounter++;
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        // Add a sink that processes the GenericRecord using AvroGenericRecordSink
        sourceStream.addSink(new AvroGenericRecordSink());

        // Execute the Flink job
        env.execute("Flink Avro Job Test");
    }
}
