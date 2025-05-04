package org.example.models;

import java.io.Serializable;
import java.util.Objects;

/**
 * POJO representing the eye gaze attention state message received from Unity via Kafka.
 * Contains the basic information: device ID, timestamp, and whether attention is detected.
 */
public class EyeGazeSensorReading implements Serializable {

    private static final long serialVersionUID = 1L;

    private String thingid;
    private String timestamp; // ISO 8601 format "yyyy-MM-ddTHH:mm:ss.fffffffZ" from C#
    private boolean attention; // true if looking at target, false otherwise

    // Default constructor for Flink/Serialization
    public EyeGazeSensorReading() {
    }

    // Getters and Setters
    public String getThingid() {
        return thingid;
    }

    public void setThingid(String thingid) {
        this.thingid = thingid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isAttention() { // Use "is" prefix for boolean getter
        return attention;
    }

    public void setAttention(boolean attention) {
        this.attention = attention;
    }

    // --- Optional: equals, hashCode, toString for debugging/testing ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EyeGazeSensorReading that = (EyeGazeSensorReading) o;
        return attention == that.attention &&
               Objects.equals(thingid, that.thingid) &&
               Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(thingid, timestamp, attention);
    }

    @Override
    public String toString() {
        return "EyeGazeSensorReading{" +
               "thingid='" + thingid + '\'' +
               ", timestamp='" + timestamp + '\'' +
               ", attention=" + attention +
               '}';
    }
}
