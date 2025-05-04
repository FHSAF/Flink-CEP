
// File: Flink-CEP/src/main/java/org/example/sinks/kafka/SmartwatchHrAlertKafkaSink.java
package org.example.sinks.kafka; // Updated package

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

// Renamed from SmartWatchAlertSink (and replaces duplicate SmartWatchKafkaSink)
public class SmartwatchHrAlertKafkaSink {
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchHrAlertKafkaSink.class);

    /**
     * Creates a configured KafkaSink for Smartwatch HR alerts (e.g., from CEP).
     *
     * @param brokers The comma-separated list of Kafka broker addresses.
     * @param topic   The target Kafka topic name for these alerts.
     * @return A configured KafkaSink<String>.
     * @throws IllegalArgumentException if brokers or topic are null or empty.
     */
    public static KafkaSink<String> getKafkaSink(String brokers, String topic) {
        // (Keep the existing getKafkaSink logic from SmartWatchAlertSink)
        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap servers cannot be null or empty.");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Target Kafka topic cannot be null or empty.");
        }

        logger.info("Configuring Smartwatch HR Alert Kafka Sink:");
        logger.info("  Brokers: {}", brokers);
        logger.info("  Topic: {}", topic);

        try {
            KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(getKafkaProperties())
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(topic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            logger.info("Successfully created Smartwatch HR Alert Kafka Sink for topic '{}'", topic);
            return kafkaSink;

        } catch (Exception e) {
            logger.error("Error creating Smartwatch HR Alert Kafka Sink for topic '{}': ", topic, e);
            throw new RuntimeException("Error creating Smartwatch HR Alert Kafka Sink", e);
        }
    }

    /**
     * Configures basic Kafka Producer properties.
     * @return Properties object for Kafka producer.
     */
    private static Properties getKafkaProperties() {
        // (Keep the existing getKafkaProperties logic)
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        return properties;
    }
}
