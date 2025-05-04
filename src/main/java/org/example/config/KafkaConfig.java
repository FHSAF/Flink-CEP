package org.example.config;

public class KafkaConfig {
    public static final String BOOTSTRAP_SERVERS = "192.168.50.105:9092,192.168.50.105:9093,192.168.50.105:9094";

    public static final String ROKOKO_TOPIC_SOURCE = "k_mocap_rokoko01_angles";
    public static final String SMARTWATCH_TOPIC01_SOURCE = "k_watch_sensor1_heartrate";
    public static final String SMARTWATCH_TOPIC02_SOURCE = "k_watch_sensor2_heartrate";
    public static final String MYONTECH_TOPIC01_SOURCE = "k_myontech_shirt01_emg";
    public static final String MYONTECH_TOPIC02_SOURCE = "k_myontech_shirt02_emg";
    public static final String MYONTECH_TOPIC03_SOURCE = "k_myontech_shirt03_emg";
    public static final String EYE_GAZE_TOPIC = "k_unity_gaze_attention";

    public static final String EMG_TOPIC_LEFT_ARM = "k_myontech_shirt01_emg";
    public static final String EMG_TOPIC_RIGHT_ARM = "k_myontech_shirt02_emg";
    public static final String EMG_TOPIC_TRUNK = "k_myontech_shirt03_emg";

    public static final String OUTPUT_TOPIC = "mocap.rokoko01-alerts";
    public static final String GROUP_ID = "flink-cep-group-rokoko";
    // public static final String SMARTWATCH_TOPIC = "ecg.smartwatch01";
    
    public static final String SMARTWATCH_GROUP_ID = "flink-cep-group-smartwatch";
    public static final String SMARTWATCH_ALERT_TOPIC = "ecg.smartwatch01-alerts";
    public static final String SMARTWATCH_ALERT_GROUP_ID = "flink-cep-group-smartwatch-alerts";
    public static final String SMARTWATCH_ALERT_SINK_TOPIC = "ecg.smartwatch01-alerts-sink";

    public static final String SUIT_TOPIC = "emg.suit01";
    public static final String SUIT_GROUP_ID = "flink-cep-group-suit";
    public static final String SUIT_ALERT_TOPIC = "emg.suit01-alerts";
    public static final String SUIT_ALERT_GROUP_ID = "flink-cep-group-suit-alerts";
    public static final String SUIT_ALERT_SINK_TOPIC = "emg.suit01-alerts-sink";
    public static final String SUIT_ALERT_SINK_GROUP_ID = "flink-cep-group-suit-alerts-sink";

    public static final String SLIDING_WINDOW_EMG_ALERT_TOPIC = "emg.suit01-sliding-window-alerts";
    public static final String SLIDING_WINDOW_EMG_ALERT_GROUP_ID = "flink-cep-group-suit-sliding-window-alerts";
    public static final String SLIDING_WINDOW_EMG_ALERT_SINK_TOPIC = "emg.suit01-sliding-window-alerts-sink";
    public static final String SLIDING_WINDOW_EMG_ALERT_SINK_GROUP_ID = "flink-cep-group-suit-sliding-window-alerts-sink";
    
    public static final String SLIDING_WINDOW_ECG_ALERT_TOPIC = "ecg.smartwatch01-sliding-window-alerts";
    public static final String SLIDING_WINDOW_ECG_ALERT_GROUP_ID = "flink-cep-group-smartwatch-sliding-window-alerts";
    public static final String SLIDING_WINDOW_ECG_ALERT_SINK_TOPIC = "ecg.smartwatch01-sliding-window-alerts-sink";
    public static final String SLIDING_WINDOW_ECG_ALERT_SINK_GROUP_ID = "flink-cep-group-smartwatch-sliding-window-alerts-sink";
}
