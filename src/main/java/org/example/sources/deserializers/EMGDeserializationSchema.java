package org.example.sources.deserializers; // Example package for deserializers

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.models.EMGSensorReading; // Import the POJO
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Custom DeserializationSchema (top-level class) to parse JSON EMG data
 * into EMGSensorReading POJOs. Implements KafkaRecordDeserializationSchema
 * and basic validation (checks for null object, null/empty thingId, null/empty timestamp).
 * Does NOT default timestamps here; passes original string to next stage.
 */
public class EMGDeserializationSchema implements KafkaRecordDeserializationSchema<EMGSensorReading> {

    private static final Logger logger = LoggerFactory.getLogger(EMGDeserializationSchema.class);
    // No explicit serialVersionUID needed unless for specific evolution needs
    private transient Gson gson; // transient: Initialize per task manager instance

    // Lazy initialization of Gson
    private Gson getGson() {
        if (gson == null) {
            gson = new Gson();
        }
        return gson;
    }

    /**
     * Deserializes the Kafka message value (byte array) into an EMGSensorReading object.
     * Performs basic validation and emits the object if valid.
     *
     * @param record The Kafka ConsumerRecord containing the message.
     * @param out    The Flink collector to emit results to.
     * @throws IOException If an I/O error occurs (though typically handled within).
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<EMGSensorReading> out) throws IOException {
        byte[] message = record.value();
        if (message == null) {
            // logger.debug("EMG Deserializer: Received null message value from topic {}", record.topic());
            return; // Skip null message values
        }

        String jsonString = null;
        try {
            jsonString = new String(message, StandardCharsets.UTF_8);
            EMGSensorReading reading = getGson().fromJson(jsonString, EMGSensorReading.class);

            // --- Validation after parsing ---
            if (reading == null) {
                logger.warn("EMG Deserializer: Gson parsed JSON to null object: {}", jsonString);
                return; // Skip if parsing results in null object
            }
            if (reading.getThingid() == null || reading.getThingid().trim().isEmpty()) {
                 logger.warn("EMG Deserializer: Parsed object is missing or has empty thingId: {}", jsonString);
                 return; // Skip emission if essential ID is missing
            }
            // Validate presence of timestamp string
            if (reading.getTimestamp() == null || reading.getTimestamp().trim().isEmpty()) {
                 logger.warn("EMG Deserializer: Parsed object is missing or has empty timestamp for thingId {}: {}", reading.getThingid(), jsonString);
                 return; // Skip emission if essential timestamp is missing
            }
            // --- End Validation ---

            // Emit the object if basic validation passes
            out.collect(reading);

        } catch (JsonSyntaxException e) {
            logger.warn("EMG Deserializer: Failed to parse JSON record from topic {}: [{}]. Error: {}", record.topic(), jsonString, e.getMessage());
            // Skip record on parsing error
        } catch (Exception e) {
             // Catch other potential errors during deserialization/validation
             logger.error("EMG Deserializer: Unexpected error deserializing record from topic {}: [{}]. Error: {}", record.topic(), jsonString, e.getMessage(), e);
             // Skip record on unexpected error
        }
    }

    /**
     * Provides Flink with the TypeInformation for the output POJO.
     * @return TypeInformation for EMGSensorReading.
     */
    @Override
    public TypeInformation<EMGSensorReading> getProducedType() {
        return TypeInformation.of(EMGSensorReading.class);
    }
}
