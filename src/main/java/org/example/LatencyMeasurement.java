package org.example;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

public class LatencyMeasurement implements Runnable {
    private final DataProducer producer;
    private final long queryIntervalMillis;

    public LatencyMeasurement(DataProducer producer, long queryIntervalMillis) {
        this.producer = producer;
        this.queryIntervalMillis = queryIntervalMillis;
    }

    @Override
    public void run() {
        while (true) {
            try {
                measureLatency();
                Thread.sleep(queryIntervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void measureLatency() {
        try (Connection connection = producer.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT MAX(timestamp) AS max_ts FROM records")) {
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                long maxTimestamp = rs.getLong("max_ts");
                long currentTime = Instant.now().toEpochMilli();
                long latency = currentTime - maxTimestamp;
                System.out.println("Query result timestamp: " + maxTimestamp + ", Current time: " + currentTime + ", Latency: " + latency + " ms");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

