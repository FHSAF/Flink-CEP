// File: Flink-CEP/src/main/java/org/example/sources/deserializer/SmartwatchDeserializationSchema.java
package org.example.sources.deserializer; // New package

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.models.SmartwatchReading; // Use correct model
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

// NEW Deserializer - Logic moved from SmartwatchSensorReadingParser
public class SmartwatchDeserializationSchema implements KafkaRecordDeserializationSchema<SmartwatchReading> {

    private static final Logger logger = LoggerFactory.getLogger(SmartwatchDeserializationSchema.class);
    private transient ObjectMapper objectMapper; // Use Jackson ObjectMapper

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<SmartwatchReading> out) throws IOException {
        byte[] message = record.value();
        if (message == null) {
            return; // Skip null messages
        }

        String jsonString = null;
        try {
            jsonString = new String(message, StandardCharsets.UTF_8);
            SmartwatchReading reading = getObjectMapper().readValue(jsonString, SmartwatchReading.class);

            // Basic Validation
            if (reading == null) {
                logger.warn("Smartwatch Deserializer: Jackson parsed JSON to null: {}", jsonString);
                return;
            }
             if (reading.getThingid() == null || reading.getThingid().trim().isEmpty()) {
                 logger.warn("Smartwatch Deserializer: Parsed object missing thingId: {}", jsonString);
                 return;
            }
             if (reading.getTimestamp() == null || reading.getTimestamp().trim().isEmpty()) {
                 logger.warn("Smartwatch Deserializer: Parsed object missing timestamp for {}: {}", reading.getThingid(), jsonString);
                 return;
            }

            logger.debug("Deserialized SmartwatchReading: {}", reading); // Use debug level
            out.collect(reading);

        } catch (Exception e) { // Catch Jackson's JsonProcessingException and others
            logger.error("Smartwatch Deserializer: Failed to parse SmartwatchReading from JSON topic {}: {}", record.topic(), jsonString, e);
            // Optionally skip or handle error differently
        }
    }

    @Override
    public TypeInformation<SmartwatchReading> getProducedType() {
        return TypeInformation.of(SmartwatchReading.class);
    }
}