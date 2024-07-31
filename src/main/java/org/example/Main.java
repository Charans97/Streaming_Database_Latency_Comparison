package org.example;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        String propertiesFilePath = "/home/charan/IdeaProjects/Blog_producer/src/main/resources/KafkaProducerConfig.properties";
        String topic = "my-topic";
        DataProducer producer = new DataProducer(1000, propertiesFilePath, topic); // Produces a record every second
        producer.start();

        Thread groundTruthThread = new Thread(new GroundTruthComputation(producer));
        groundTruthThread.start();

        Thread aggregationThread = new Thread(new AggregationQuery(producer));
        aggregationThread.start();
    }
}

