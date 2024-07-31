package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
    private final Connection connection;

    public DataProducer(long rateInMillis, String propertiesFilePath, String topic) throws IOException, SQLException {
        this.records = new CopyOnWriteArrayList<>();
        this.timer = new Timer(true);
        this.rateInMillis = rateInMillis;

        Properties properties = new Properties();
        properties.load(new FileInputStream(propertiesFilePath));
        this.producer = new KafkaProducer<>(properties);
        this.topic = topic;

        // Initialize database connection
        this.connection = DriverManager.getConnection("jdbc:h2:mem:testdb", "sa", "1111");
        initializeDatabase();
    }

    private void initializeDatabase() throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(
                "CREATE TABLE records (timestamp BIGINT PRIMARY KEY)")) {
            stmt.execute();
        }
    }

    public void start() {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long timestamp = Instant.now().toEpochMilli();
                Record record = new Record(timestamp);
                records.add(record);
                producer.send(new ProducerRecord<>(topic, Long.toString(timestamp)));
                storeRecordInDatabase(timestamp);
                System.out.println("Produced: " + record);
            }
        }, 0, rateInMillis);
    }

    private void storeRecordInDatabase(long timestamp) {
        try (PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO records (timestamp) VALUES (?)")) {
            stmt.setLong(1, timestamp);
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<Record> getRecords() {
        return records;
    }

    public Connection getConnection() {
        return connection;
    }

    public static void main(String[] args) throws IOException, SQLException {
        String propertiesFilePath = "/home/charan/IdeaProjects/Blog_producer/src/main/resources/KafkaProducerConfig.properties";
        String topic = "my-topic";
        DataProducer producer = new DataProducer(1000, propertiesFilePath, topic); // Produces a record every second
        producer.start();
    }
}

