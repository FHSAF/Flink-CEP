package org.example.processing;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.models.SensorReading;
import org.example.models.RebaScore;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class RebaMapper implements MapFunction<SensorReading, String> {

    private transient MoCapREBAProcessing calculator;
    private transient Gson gson;

    // Task-specific context - these MUST be set appropriately for the analysis
    private final double loadKg;
    private final int couplingScore;
    private final int activityScore;

    // Constructor to pass task context
    public RebaMapper(double loadKg, int couplingScore, int activityScore) {
        this.loadKg = loadKg;
        this.couplingScore = couplingScore;
        this.activityScore = activityScore; // Note: activityScore=1 is typical assumption for dynamic task part
    }

    // Lazy initialization on the Task Manager
    private void ensureInitialized() {
        if (calculator == null) {
            // Instantiate calculator - Modifiers are passed during calculation now
            calculator = new MoCapREBAProcessing();
        }
         if (gson == null) {
            gson = new GsonBuilder().create();
        }
    }

    @Override
    public String map(SensorReading sensorReading) throws Exception {
        ensureInitialized();
        if (sensorReading == null) return null;

        // Calculate score using the reading and the task context modifiers
        RebaScore rebaScore = calculator.calculateScore(sensorReading, this.loadKg, this.couplingScore, this.activityScore);

        if (rebaScore != null) {
            // System.out.println("✅ REBA Calculated: " + rebaScore.toString()); // Debug logging if needed
            return gson.toJson(rebaScore);
        } else {
            return null; // Or return JSON indicating error
        }
    }
}