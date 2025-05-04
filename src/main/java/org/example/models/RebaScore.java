package org.example.models; // Or your preferred package

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

// Make sure this class is public if used across packages
public class RebaScore implements Serializable {
    private static final long serialVersionUID = 1L; // Good practice for Serializable

    public String thingId;
    public String timestamp; // Original timestamp string

    // Component Scores
    public int neckScore;
    public int trunkScore;
    public int legScore;
    public int upperArmLeftScore;
    public int upperArmRightScore;
    public int lowerArmLeftScore;
    public int lowerArmRightScore;
    public int wristLeftScore;
    public int wristRightScore;

    // Group & Table Scores
    public int groupAScore; // Score from Table A (Trunk, Neck, Legs)
    public int groupBScore; // Score from Table B (Arms, Wrists)
    public int tableCScore; // Score from Table C

    // Modifiers
    public int loadForceScore; // Modifier for load/force
    public int couplingScore;  // Modifier for grip quality
    public int activityScore;  // Modifier for static/dynamic/repetition

    // Final Results
    public int finalRebaScore;
    public String riskLevel;

    // Optional: Details for debugging/analysis
    public Map<String, Object> calculationDetails = new HashMap<>();

    // Default constructor for Flink/Serialization
    public RebaScore() {}

    @Override
    public String toString() {
        return "RebaScore{" +
               "thingId='" + thingId + '\'' +
               ", timestamp='" + timestamp + '\'' +
               ", neckScore=" + neckScore +
               ", trunkScore=" + trunkScore +
               ", legScore=" + legScore +
               ", upperArmLeftScore=" + upperArmLeftScore +
               ", upperArmRightScore=" + upperArmRightScore +
               ", lowerArmLeftScore=" + lowerArmLeftScore +
               ", lowerArmRightScore=" + lowerArmRightScore +
               ", wristLeftScore=" + wristLeftScore +
               ", wristRightScore=" + wristRightScore +
               ", groupAScore=" + groupAScore +
               ", groupBScore=" + groupBScore +
               ", tableCScore=" + tableCScore +
               ", loadForceScore=" + loadForceScore +
               ", couplingScore=" + couplingScore +
               ", activityScore=" + activityScore +
               ", finalRebaScore=" + finalRebaScore +
               ", riskLevel='" + riskLevel + '\'' +
               ", details=" + calculationDetails +
               '}';
    }
}