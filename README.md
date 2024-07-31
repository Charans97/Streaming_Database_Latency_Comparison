pom.xml: 
Includes Kafka and H2 database dependencies.

KafkaProducerConfig.properties: 
Contains Kafka configuration.

Record.java: 
Simple POJO to represent a record.

DataProducer.java: 
Produces records to Kafka and stores them in an H2 in-memory database.

GroundTruthComputation.java: 
Computes ground truth values.

AggregationQuery.java: 
Aggregates records in a sliding 10-second window.

LatencyMeasurement.java: 
Measures the latency of pull queries against the database.

Main.java: 
Starts the producer, ground truth computation, aggregation query, and latency measurement threads.

This complete setup should enable you to produce data, compute ground truth, perform aggregation queries, and measure query latency effectively. Ensure you have Kafka and the database set up and running correctly before executing the code.
