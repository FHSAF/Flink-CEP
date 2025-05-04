package org.example.models;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartwatchSensorReading {
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchSensorReading.class);

    private String thingid;
    private int heartrate;
    private String timestamp;

    public SmartwatchSensorReading() {}

    public SmartwatchSensorReading(String thingid, int heartrate, String timestamp) {
        this.thingid = thingid;
        this.heartrate = heartrate;
        this.timestamp = timestamp;
        logger.info("Created SmartwatchSensorReading: {}", this);
    }

    // Getters
    public String getThingid() { return thingid; }
    public int getHeartrate() { return heartrate; }
    public String getTimestamp() { return timestamp; }

    // Setters
    public void setThingid(String thingid) { this.thingid = thingid; }
    public void setHeartrate(int heartrate) { this.heartrate = heartrate; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "SmartwatchSensorReading{" +
                "thingid='" + thingid + '\'' +
                ", heartrate=" + heartrate +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
