package org.example.config;

public class DBConfig {
	public static final String DB_URL = "jdbc:postgresql://192.168.50.208:5432/";
	public static final String DB_USER = "postgres";
	public static final String DB_PASSWORD = "postgres";
	// Raw data tables
	public static final String ROKOKO_DB_NAME = "rokoko_db";
	public static final String ROKOKO_DB_TABLE = "MoCapRawData";
	// Sliding window tables
	public static final String ROKOKO_AVERAGE_DB_NAME = "rokoko_average_alerts_db";
	public static final String ROKOKO_AVERAGE_DB_TABLE = "AverageAngleAlerts";

	public static final String ROKOKO_REBA_SCORE_DB_NAME = "rebascore_db";
	public static final String ROKOKO_REBA_SCORE_DB_TABLE = "RebaScores";
	// Raw data tables
	public static final String SMARTWATCH_DB_NAME = "smartwatch_db";
	public static final String SMARTWATCH_DB_TABLE = "SmartwatchRawData";

	public static final String GAZE_STATE_DB_NAME = "gaze_state_db";
	public static final String GAZE_STATE_TABLE = "EyeGazeRawData";
	public static final String GAZE_STATE_ALERT_TABLE = "EyeGazeAttentionAlerts";
	// Sliding window tables
	public static final String SMARTWATCH_AVERAGE_DB_NAME = "smartwatch_average_db";
	public static final String SMARTWATCH_AVERAGE_DB_TABLE = "smartwatch_average";
	// Raw data tables
	public static final String EMG_DB_NAME = "emg_db";
	public static final String EMG_DB_TABLE_01 = "emg_raw_data_01";
	public static final String EMG_DB_TABLE_02 = "emg_raw_data_02";
	public static final String EMG_DB_TABLE_03 = "emg_raw_data_03";
	// Sliding window tables
	public static final String EMG_AVERAGE_DB_NAME = "emg_average_db";
	public static final String EMG_AVERAGE_DB_TABLE = "emg_fatigue_alerts";
}