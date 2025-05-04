package org.example.sinks;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
// Import the correct DeliveryGuarantee
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides configuration for a Flink Kafka Sink targeting EMG Fatigue Alerts.
 */
public class EMGKafkaSink {

    private static final Logger logger = LoggerFactory.getLogger(EMGKafkaSink.class);

    private static final String DEFAULT_EMG_FATIGUE_ALERT_TOPIC = "emg_fatigue_alerts";

    /**
     * Creates a Flink KafkaSink for EMG Fatigue Alert JSON strings.
     *
     * @param brokers   Comma-separated list of Kafka broker addresses.
     * @param topic     The specific Kafka topic to write alerts to. If null, uses default.
     * @return Configured KafkaSink<String>
     */
    public static KafkaSink<String> getKafkaSink(String brokers, String topic) {
        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap servers cannot be null or empty for EMGKafkaSink");
        }

        String targetTopic = (topic != null && !topic.isEmpty()) ? topic : DEFAULT_EMG_FATIGUE_ALERT_TOPIC;
        logger.info("Configuring Kafka Sink for EMG Fatigue Alerts. Brokers: {}, Topic: {}", brokers, targetTopic);

        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(targetTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                // Use the correctly imported DeliveryGuarantee enum
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /**
     * Creates a Flink KafkaSink using the default topic name.
     * @param brokers Comma-separated list of Kafka broker addresses.
     * @return Configured KafkaSink<String>
     */
    public static KafkaSink<String> getKafkaSink(String brokers) {
        return getKafkaSink(brokers, DEFAULT_EMG_FATIGUE_ALERT_TOPIC);
    }
}