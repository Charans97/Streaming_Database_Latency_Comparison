package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataProducer {
    private final List<Record> records;
    private final Timer timer;
    private final long rateInMillis;
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public DataProducer(long rateInMillis, String propertiesFilePath, String topic) throws IOException {
        this.records = new CopyOnWriteArrayList<>();
        this.timer = new Timer(true);
        this.rateInMillis = rateInMillis;

        Properties properties = new Properties();
        properties.load(new FileInputStream(propertiesFilePath));
        this.producer = new KafkaProducer<>(properties);
        this.topic = topic;
    }

    public void start() {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long timestamp = Instant.now().toEpochMilli();
                Record record = new Record(timestamp);
                records.add(record);
                producer.send(new ProducerRecord<>(topic, Long.toString(timestamp)));
                System.out.println("Produced: " + record);
            }
        }, 0, rateInMillis);
    }

    public List<Record> getRecords() {
        return records;
    }

    public static void main(String[] args) throws IOException {
        String propertiesFilePath = "/home/charan/IdeaProjects/Blog_producer/src/main/resourcesKafkaProducerConfig.properties";
        String topic = "my-topic";
        DataProducer producer = new DataProducer(1000, propertiesFilePath, topic); // Produces a record every second
        producer.start();
    }
}

