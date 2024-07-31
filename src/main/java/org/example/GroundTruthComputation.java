package org.example;

import java.io.IOException;
import java.util.List;

public class GroundTruthComputation implements Runnable {
    private final DataProducer producer;

    public GroundTruthComputation(DataProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        while (true) {
            List<Record> records = producer.getRecords();
            // Implement your ground truth computation logic here
            System.out.println("Computed ground truth for " + records.size() + " records");
            try {
                Thread.sleep(10000); // Sleep for 10 seconds before next computation
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String propertiesFilePath = "/home/charan/IdeaProjects/Blog_producer/src/main/resources/KafkaProducerConfig.properties";
        String topic = "my-topic";
        DataProducer producer = new DataProducer(1000, propertiesFilePath, topic);
        producer.start();
        Thread groundTruthThread = new Thread(new GroundTruthComputation(producer));
        groundTruthThread.start();
    }
}
