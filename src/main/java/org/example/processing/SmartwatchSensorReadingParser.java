package org.example.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.models.SmartwatchSensorReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartwatchSensorReadingParser {
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchSensorReadingParser.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static SmartwatchSensorReading parse(String json) {
        try {
            SmartwatchSensorReading reading = objectMapper.readValue(json, SmartwatchSensorReading.class);
            logger.info("Parsed SmartwatchSensorReading: {}", reading);
            return reading;
        } catch (Exception e) {
            logger.error("Failed to parse SmartwatchSensorReading from JSON: {}", json, e);
            return null;
        }
    }
}
