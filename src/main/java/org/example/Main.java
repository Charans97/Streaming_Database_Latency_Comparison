package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        String propertiesFilePath = "/home/charan/IdeaProjects/Blog_producer/src/main/resources/KafkaProducerConfig.properties";
        String topic = "my-topic";

        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(propertiesFilePath)) {
            properties.load(input);
            String productionTimeString = properties.getProperty("production.time");
            long productionTime = Long.parseLong(productionTimeString);
            DataProducer producer = new DataProducer(productionTime, propertiesFilePath, topic); // Produces a record every second(1000)
            producer.start();

            Thread groundTruthThread = new Thread(new GroundTruthComputation(producer));
            groundTruthThread.start();

            Thread aggregationThread = new Thread(new AggregationQuery(producer));
            aggregationThread.start();

            LatencyMeasurement latencyMeasurement = new LatencyMeasurement(producer, 1000); // Query every second
            Thread latencyThread = new Thread(latencyMeasurement);
            latencyThread.start();
        }
    }
}

