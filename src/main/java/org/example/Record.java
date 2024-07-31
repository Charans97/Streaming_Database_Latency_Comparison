package org.example;

public class Record {
    private final long timestamp;

    public Record(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Record{" +
                "timestamp=" + timestamp +
                '}';
    }
}


