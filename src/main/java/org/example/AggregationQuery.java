package org.example;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class AggregationQuery implements Runnable {
    private final DataProducer producer;

    public AggregationQuery(DataProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        while (true) {
            long currentTime = Instant.now().toEpochMilli();
            long windowStartTime = currentTime - 10000; // 10-second window

            List<Record> records = producer.getRecords();
            long count = records.stream()
                    .filter(record -> record.getTimestamp() >= windowStartTime)
                    .count();

            System.out.println("Aggregated " + count + " records in the last 10 seconds");

            try {
                Thread.sleep(1000); // Sleep for 1 second before next aggregation
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }


        public static void main(String[] args) throws IOException {
            String propertiesFilePath = "/home/charan/IdeaProjects/Blog_producer/src/main/resourcesKafkaProducerConfig.properties";
            String topic = "my-topic";
            DataProducer producer = new DataProducer(1000, propertiesFilePath, topic);
            producer.start();
            Thread aggregationThread = new Thread(new AggregationQuery(producer));
            aggregationThread.start();

    }
}
