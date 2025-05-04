// File: Flink-CEP/src/main/java/org/example/models/SmartwatchReading.java
package org.example.models;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable; // Import Serializable

// Renamed from SmartwatchSensorReading
public class SmartwatchReading implements Serializable { // Implement Serializable
    private static final long serialVersionUID = 103L; // Unique ID
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchReading.class);

    // Field names MUST match the incoming JSON keys from Kafka
    private String thingid;
    private int heartrate;
    private String timestamp; // Original timestamp string "yyyy-MM-dd HH:mm:ss"

    // Default constructor required by Flink
    public SmartwatchReading() {}

    // Constructor with all fields (optional)
    public SmartwatchReading(String thingid, int heartrate, String timestamp) {
        this.thingid = thingid;
        this.heartrate = heartrate;
        this.timestamp = timestamp;
        // Removed logger call from constructor
    }

    // Getters
    public String getThingid() { return thingid; }
    public int getHeartrate() { return heartrate; }
    public String getTimestamp() { return timestamp; }

    // Setters
    public void setThingid(String thingid) { this.thingid = thingid; }
    public void setHeartrate(int heartrate) { this.heartrate = heartrate; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    // toString (optional, for debugging)
    @Override
    public String toString() {
        // (Keep the existing toString method from SmartwatchSensorReading.java)
        return "SmartwatchReading{" +
                "thingid='" + thingid + '\'' +
                ", heartrate=" + heartrate +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}