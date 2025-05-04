// File: Flink-CEP/src/main/java/org/example/sources/provider/SmartwatchKafkaSourceProvider.java
package org.example.sources.provider; // Updated package

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.example.config.KafkaConfig;
import org.example.models.SmartwatchReading; // Import Correct Model
import org.example.sources.deserializer.SmartwatchDeserializationSchema; // Import New Deserializer
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Renamed from SmartwatchSourceProvider
public class SmartwatchKafkaSourceProvider {
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchKafkaSourceProvider.class);

     /**
     * Creates and configures a KafkaSource for reading Smartwatch data.
     * Uses SmartwatchDeserializationSchema to parse JSON into SmartwatchReading objects.
     *
     * @return Configured KafkaSource<SmartwatchReading>
     * @throws IllegalStateException if required KafkaConfig values are missing.
     */
    public static KafkaSource<SmartwatchReading> getSmartwatchSource() { // Return type updated
        try {
            logger.info("========= Initializing Smartwatch Kafka Source Configuration =========");
            logger.info("  Brokers: {}", KafkaConfig.BOOTSTRAP_SERVERS);
            // Assuming a single primary source topic for smartwatch HR
            String topic = KafkaConfig.SMARTWATCH_TOPIC01_SOURCE;
            // Attempt to use a specific group ID if defined, otherwise fallback
            String groupId = KafkaConfig.SMARTWATCH_GROUP_ID != null ? KafkaConfig.SMARTWATCH_GROUP_ID : KafkaConfig.GROUP_ID + "-smartwatch";

            logger.info("  Topic: {}", topic);
            logger.info("  Group ID: {}", groupId);
            logger.info("======================================================================");

            if (KafkaConfig.BOOTSTRAP_SERVERS == null || topic == null || groupId == null) {
                 throw new IllegalStateException("Kafka configuration (servers, smartwatch topic, group id) missing in KafkaConfig.");
            }

            KafkaSource<SmartwatchReading> kafkaSource = KafkaSource.<SmartwatchReading>builder() // Output type SmartwatchReading
                    .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                    .setTopics(topic)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    // Use the new Deserialization Schema
                    .setDeserializer(new SmartwatchDeserializationSchema()) // UPDATED
                    .build();

            logger.info("Smartwatch Kafka Source configured successfully.");
            return kafkaSource;

        } catch (Exception e) {
            logger.error("Error initializing Smartwatch Kafka Source", e);
             throw new RuntimeException("Error initializing Smartwatch Kafka Source", e);
        }
    }
}