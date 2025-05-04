// File: Flink-CEP/src/main/java/org/example/processing/mocap/MoCapRebaProcessor.java
package org.example.processing.mocap; // New package

import org.apache.flink.api.common.functions.MapFunction;
import org.example.models.MoCapReading; // Updated model import
import org.example.models.RebaScore;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

// Combines RebaMapper and MoCapREBAProcessing logic
public class MoCapRebaProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MoCapRebaProcessor.class);

    // --- REBA Tables (Keep as defined in MoCapREBAProcessing.java) ---
    private static final int[][][] TABLE_A = { /* ... Copy from MoCapREBAProcessing ... */
        // Trunk Score = 1
        {{1, 2, 3, 4}, {1, 2, 3, 4}, {3, 3, 5, 6}}, // Neck Scores 1, 2, 3 (Rows) vs Leg Scores 1, 2, 3, 4 (Cols)
        // Trunk Score = 2
        {{2, 3, 4, 5}, {3, 4, 5, 6}, {4, 5, 6, 7}},
        // Trunk Score = 3
        {{2, 4, 5, 6}, {4, 5, 6, 7}, {5, 6, 7, 8}},
        // Trunk Score = 4
        {{3, 5, 6, 7}, {5, 6, 7, 8}, {6, 7, 8, 9}},
        // Trunk Score = 5
        {{4, 6, 7, 8}, {6, 7, 8, 9}, {7, 8, 9, 9}}
    };
    private static final int[][][] TABLE_B = { /* ... Copy from MoCapREBAProcessing ... */
        // Upper Arm Score = 1
        {{1, 2, 2, 3}, {1, 2, 3, 4}}, // Lower Arm Scores 1, 2 (Rows) vs Wrist Scores 1, 2, 3, 4 (Cols)
        // Upper Arm Score = 2
        {{1, 2, 3, 4}, {2, 3, 4, 5}},
        // Upper Arm Score = 3
        {{3, 4, 5, 5}, {4, 5, 5, 6}},
        // Upper Arm Score = 4
        {{4, 5, 5, 6}, {5, 6, 6, 7}},
        // Upper Arm Score = 5
        {{6, 7, 7, 7}, {7, 7, 7, 8}},
        // Upper Arm Score = 6
        {{7, 7, 7, 8}, {8, 8, 8, 9}}
    };
    private static final int[][] TABLE_C = { /* ... Copy from MoCapREBAProcessing ... */
      // B-> 1  2  3  4  5  6  7  8  9 10 11 12
          { 1, 1, 1, 2, 3, 3, 4, 5, 6, 7, 7, 7}, // A = 1
          { 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 7, 8}, // A = 2
          { 2, 3, 3, 3, 4, 5, 6, 7, 7, 8, 8, 8}, // A = 3
          { 3, 4, 4, 4, 5, 6, 7, 8, 8, 9, 9, 9}, // A = 4
          { 4, 4, 4, 5, 6, 7, 8, 8, 9, 9, 9, 9}, // A = 5
          { 4, 4, 5, 6, 7, 8, 8, 9, 9,10,10,10}, // A = 6
          { 5, 5, 6, 7, 8, 8, 9, 9,10,10,10,10}, // A = 7
          { 5, 5, 6, 7, 8, 9, 9,10,10,10,10,10}, // A = 8
          { 6, 6, 7, 8, 8, 9,10,10,10,10,10,10}, // A = 9
          { 6, 6, 7, 8, 9, 9,10,10,10,10,10,10}, // A = 10
          { 7, 7, 7, 8, 9,10,10,10,10,10,10,10}, // A = 11
          { 7, 7, 7, 8, 9,10,10,10,10,10,10,10}  // A = 12
    };
    // --- End REBA Tables ---


    /**
     * Calculates the REBA score based on MoCapReading data and provided task context.
     * (Combined logic from MoCapREBAProcessing.calculateScore)
     * @param s MoCapReading object containing joint angles.
     * @param loadKg Load handled in kilograms.
     * @param couplingScore Score from REBA Coupling assessment (0-3).
     * @param activityScore Score from REBA Activity assessment (0 or 1).
     * @return RebaScore object containing detailed results.
     */
    private RebaScore calculateRebaScoreInternal(MoCapReading s, double loadKg, int couplingScore, int activityScore) {
        if (s == null) return null;

        RebaScore score = new RebaScore();
        score.thingId = s.getThingid();
        score.timestamp = s.getTimestamp(); // Keep original string timestamp

        try {
            // --- Step 1-3: Neck, Trunk, Leg Scores ---
            score.neckScore = calculateNeckScore(s, score.calculationDetails);
            score.trunkScore = calculateTrunkScore(s, score.calculationDetails);
            score.legScore = calculateLegScore(s, score.calculationDetails);

            // --- Step 4: Table A Lookup ---
            score.groupAScore = lookupTableA(score.trunkScore, score.neckScore, score.legScore);
            score.calculationDetails.put("lookup_table_a_score", score.groupAScore);

            // --- Step 5: Load/Force Score ---
            score.loadForceScore = calculateLoadForceScore(loadKg, false); // Assume no shock force
            score.calculationDetails.put("load_kg", loadKg);

            // --- Step 6: Score A ---
            int scoreA = score.groupAScore + score.loadForceScore;
            score.calculationDetails.put("score_A_total", scoreA);

            // --- Step 7-9: Upper Arm, Lower Arm, Wrist Scores ---
            score.upperArmLeftScore = calculateUpperArmScore(s.getShoulderFlexExtLeft(), s.getShoulderAbdAddLeft(), s.getUpperarmRotationLeft(), false, score.calculationDetails, "L");
            score.upperArmRightScore = calculateUpperArmScore(s.getShoulderFlexExtRight(), s.getShoulderAbdAddRight(), s.getUpperarmRotationRight(), false, score.calculationDetails, "R");
            int contributingUpperArmScore = Math.max(score.upperArmLeftScore, score.upperArmRightScore);
            score.calculationDetails.put("contributing_upper_arm_score", contributingUpperArmScore);

            score.lowerArmLeftScore = calculateLowerArmScore(s.getElbowFlexExtLeft(), score.calculationDetails, "L");
            score.lowerArmRightScore = calculateLowerArmScore(s.getElbowFlexExtRight(), score.calculationDetails, "R");
            int contributingLowerArmScore = Math.max(score.lowerArmLeftScore, score.lowerArmRightScore);
            score.calculationDetails.put("contributing_lower_arm_score", contributingLowerArmScore);

            score.wristLeftScore = calculateWristScore(s.getHandFlexExtLeft(), s.getHandRadialUlnarLeft(), s.getLowerarmPronSupLeft(), score.calculationDetails, "L");
            score.wristRightScore = calculateWristScore(s.getHandFlexExtRight(), s.getHandRadialUlnarRight(), s.getLowerarmPronSupRight(), score.calculationDetails, "R");
            int contributingWristScore = Math.max(score.wristLeftScore, score.wristRightScore);
            score.calculationDetails.put("contributing_wrist_score", contributingWristScore);

            // --- Step 10: Table B Lookup ---
            score.groupBScore = lookupTableB(contributingUpperArmScore, contributingLowerArmScore, contributingWristScore);
            score.calculationDetails.put("lookup_table_b_score", score.groupBScore);

            // --- Step 11: Coupling Score ---
            score.couplingScore = couplingScore; // Use provided value
            score.calculationDetails.put("coupling_score_input", couplingScore);

            // --- Step 12: Score B ---
            int scoreB = score.groupBScore + score.couplingScore;
            score.calculationDetails.put("score_B_total", scoreB);

            // --- Step 13: Table C Lookup ---
            score.tableCScore = lookupTableC(scoreA, scoreB);
            score.calculationDetails.put("lookup_table_c_score", score.tableCScore);

            // --- Step 14: Activity Score ---
            score.activityScore = activityScore;
            score.calculationDetails.put("activity_score_input", activityScore);

            // --- Step 15: Final Score & Risk ---
            score.finalRebaScore = score.tableCScore + score.activityScore;
            score.riskLevel = getRiskLabel(score.finalRebaScore);

        } catch (Exception e) {
            logger.error("Error calculating REBA score for thingId {}: {}", s.getThingid(), e.getMessage(), e);
            score.riskLevel = "Calculation Error";
            score.finalRebaScore = -1;
        }
        return score;
    }

    // --- Detailed Scoring Functions (Copy from MoCapREBAProcessing.java) ---
    private int calculateTrunkScore(MoCapReading s, Map<String, Object> details) { /* ... Copy ... */
        double tilt = s.getTorsoTilt(); int score;
        if (Math.abs(tilt) <= 5) score = 1;
        else if ((tilt > 5 && tilt <= 20) || (tilt < -5 && tilt >= -20)) score = 2;
        else if ((tilt > 20 && tilt <= 60) || tilt < -20) score = 3;
        else score = 4;
        details.put("trunk_posture_score", score);
        if (isTrunkTwisted(s.getBackTorsion()) || isTrunkSideBent(s.getTorsoSideTilt())) { score += 1; details.put("trunk_adjust", 1); } else { details.put("trunk_adjust", 0); }
        return score;
     }
    private int calculateNeckScore(MoCapReading s, Map<String, Object> details) { /* ... Copy ... */
        double flexExt = s.getNeckFlexExt(); int score;
        if (flexExt >= 0 && flexExt <= 20) score = 1; else score = 2;
        details.put("neck_posture_score", score);
        if (isNeckTwisted(s.getNeckTorsion()) || isNeckSideBent(s.getHeadTilt())) { score += 1; details.put("neck_adjust", 1); } else { details.put("neck_adjust", 0); }
        return score;
    }
    private int calculateLegScore(MoCapReading s, Map<String, Object> details) { /* ... Copy ... */
         int score = 1; details.put("leg_base_support_score", score);
         double maxKneeFlex = Math.max(Math.abs(s.getKneeFlexExtLeft()), Math.abs(s.getKneeFlexExtRight())); int adjustment = 0;
         if (maxKneeFlex > 60) adjustment = 2; else if (maxKneeFlex > 30) adjustment = 1;
         details.put("leg_adjust_knee_flex", adjustment); return score + adjustment;
    }
    private int calculateUpperArmScore(double flexExt, double abdAdd, double rotation, boolean isSupported, Map<String, Object> details, String side) { /* ... Copy ... */
         int score; double absFlex = Math.abs(flexExt);
         if (absFlex <= 20) score = 1; else if (absFlex <= 45) score = 2; else if (absFlex <= 90) score = 3; else score = 4;
         details.put("upper_arm_" + side + "_flex_score", score); int adjustment = 0;
         if (flexExt < -20) { adjustment += 1; details.put("upper_arm_" + side + "_adjust_extension", 1); }
         if (Math.abs(abdAdd) > 45) { adjustment += 1; details.put("upper_arm_" + side + "_adjust_abd", 1); }
         if (isSupported) { adjustment -= 1; details.put("upper_arm_" + side + "_adjust_support", -1); }
         details.put("upper_arm_" + side + "_total_adjust", adjustment); return Math.max(1, score + adjustment);
    }
    private int calculateLowerArmScore(double elbowFlexExt, Map<String, Object> details, String side) { /* ... Copy ... */
         int score; if (elbowFlexExt >= 60 && elbowFlexExt <= 100) score = 1; else score = 2;
         details.put("lower_arm_" + side + "_score", score); return score;
    }
    private int calculateWristScore(double flexExt, double radialUlnar, double pronSup, Map<String, Object> details, String side) { /* ... Copy ... */
        int score; if (Math.abs(flexExt) <= 15) score = 1; else score = 2;
        details.put("wrist_" + side + "_flex_score", score);
        if (Math.abs(radialUlnar) > 10 || Math.abs(pronSup) > 45) { score += 1; details.put("wrist_" + side + "_adjust_dev_twist", 1); } else { details.put("wrist_" + side + "_adjust_dev_twist", 0); }
        return score;
    }
    private int calculateLoadForceScore(double loadKg, boolean isShockForce) { /* ... Copy ... */
         int score = 0; if (loadKg > 10) score = 2; else if (loadKg >= 5) score = 1; else score = 0;
         if (isShockForce) score += 1; return score;
    }
    // --- Table Lookup Helpers (Copy from MoCapREBAProcessing.java) ---
    private int lookupTableA(int trunkScore, int neckScore, int legScore) { /* ... Copy ... */
        int trunkIdx = Math.max(0, Math.min(TABLE_A.length - 1, trunkScore - 1)); int neckIdx = Math.max(0, Math.min(TABLE_A[0].length - 1, neckScore - 1)); int legIdx = Math.max(0, Math.min(TABLE_A[0][0].length - 1, legScore - 1));
        return TABLE_A[trunkIdx][neckIdx][legIdx];
    }
    private int lookupTableB(int upperArmScore, int lowerArmScore, int wristScore) { /* ... Copy ... */
        int uArmIdx = Math.max(0, Math.min(TABLE_B.length - 1, upperArmScore - 1)); int lArmIdx = Math.max(0, Math.min(TABLE_B[0].length - 1, lowerArmScore - 1)); int wristIdx = Math.max(0, Math.min(TABLE_B[0][0].length - 1, wristScore - 1));
        return TABLE_B[uArmIdx][lArmIdx][wristIdx];
    }
    private int lookupTableC(int scoreA, int scoreB) { /* ... Copy ... */
        int scoreAIdx = Math.max(0, Math.min(TABLE_C.length - 1, scoreA - 1)); int scoreBIdx = Math.max(0, Math.min(TABLE_C[0].length - 1, scoreB - 1));
        return TABLE_C[scoreAIdx][scoreBIdx];
    }
    // --- Helper Predicates (Copy from MoCapREBAProcessing.java) ---
    private boolean isTrunkTwisted(double torsoTorsion) { return Math.abs(torsoTorsion) > 10; }
    private boolean isTrunkSideBent(double torsoSideTilt) { return Math.abs(torsoSideTilt) > 10; }
    private boolean isNeckTwisted(double neckTorsion) { return Math.abs(neckTorsion) > 15; }
    private boolean isNeckSideBent(double headTilt) { return Math.abs(headTilt) > 15; }
    // --- Risk Level Mapping (Copy from MoCapREBAProcessing.java) ---
    private String getRiskLabel(int finalScore) { /* ... Copy ... */
        if (finalScore == 1) return "Negligible"; if (finalScore <= 3) return "Low"; if (finalScore <= 7) return "Medium"; if (finalScore <= 10) return "High"; return "Very High";
    }

    // --- Flink MapFunction (Replaces original RebaMapper class) ---
    public static class RebaScoreMapFunction implements MapFunction<MoCapReading, String> {
        private transient MoCapRebaProcessor calculator; // Instance of the outer class logic
        private transient Gson gson;
        private final double loadKg;
        private final int couplingScore;
        private final int activityScore;

        public RebaScoreMapFunction(double loadKg, int couplingScore, int activityScore) {
            this.loadKg = loadKg;
            this.couplingScore = couplingScore;
            this.activityScore = activityScore;
        }

        private void ensureInitialized() {
            if (calculator == null) { calculator = new MoCapRebaProcessor(); } // Instantiate calculator
            if (gson == null) { gson = new GsonBuilder().create(); }
        }

        @Override
        public String map(MoCapReading moCapReading) throws Exception {
            ensureInitialized();
            if (moCapReading == null) return null;
            RebaScore rebaScore = calculator.calculateRebaScoreInternal(moCapReading, this.loadKg, this.couplingScore, this.activityScore);
            return (rebaScore != null) ? gson.toJson(rebaScore) : null;
        }
    }
}