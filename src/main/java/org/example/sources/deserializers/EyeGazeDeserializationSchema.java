package org.example.sources.deserializers; // Or your chosen package

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.models.EyeGazeSensorReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class EyeGazeDeserializationSchema implements KafkaRecordDeserializationSchema<EyeGazeSensorReading> {

    private static final Logger logger = LoggerFactory.getLogger(EyeGazeDeserializationSchema.class);
    private transient Gson gson;

    private Gson getGson() {
        if (gson == null) { gson = new Gson(); }
        return gson;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<EyeGazeSensorReading> out) throws IOException {
        byte[] message = record.value();
        if (message == null) {
            return; // Skip null message values
        }

        String jsonString = null; // Declare outside try for logging scope
        try {
            jsonString = new String(message, StandardCharsets.UTF_8);

            // --- LOG RAW MESSAGE HERE ---
            // Use INFO or DEBUG level depending on how much logging you want
            logger.info("EyeGaze Deserializer: Received raw message from Kafka topic {}: {}", record.topic(), jsonString);
            // --- END LOG ---

            EyeGazeSensorReading reading = getGson().fromJson(jsonString, EyeGazeSensorReading.class);

            // Validation after parsing
            if (reading == null) {
                logger.warn("EyeGaze Deserializer: Gson parsed JSON to null object: {}", jsonString);
                return;
            }
            if (reading.getThingid() == null || reading.getThingid().trim().isEmpty()) {
                 logger.warn("EyeGaze Deserializer: Parsed object is missing or has empty thingId: {}", jsonString);
                 return;
            }
            if (reading.getTimestamp() == null || reading.getTimestamp().trim().isEmpty()) {
                 logger.warn("EyeGaze Deserializer: Parsed object is missing or has empty timestamp for thingId {}: {}", reading.getThingid(), jsonString);
                 return;
            }

            // Emit the object if basic validation passes
            out.collect(reading);

        } catch (JsonSyntaxException e) {
            logger.warn("EyeGaze Deserializer: Failed to parse JSON record from topic {}: [{}]. Error: {}", record.topic(), jsonString, e.getMessage());
        } catch (Exception e) {
             logger.error("EyeGaze Deserializer: Unexpected error deserializing record from topic {}: [{}]. Error: {}", record.topic(), jsonString, e.getMessage(), e);
        }
    }

    @Override
    public TypeInformation<EyeGazeSensorReading> getProducedType() {
        return TypeInformation.of(EyeGazeSensorReading.class);
    }
}
