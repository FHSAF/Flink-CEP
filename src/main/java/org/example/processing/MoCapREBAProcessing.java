package org.example.processing;

import org.example.models.SensorReading;
import org.example.models.RebaScore; // Assuming RebaScore POJO from previous answer exists
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Calculates REBA score based on instantaneous SensorReading data.
 * NOTE: Requires verification of all angle brackets, scores, adjustments,
 * and table lookups against the official REBA standard/worksheet.
 * Load, Coupling, and Activity scores usually require external input.
 */
public class MoCapREBAProcessing {

    private static final Logger logger = LoggerFactory.getLogger(MoCapREBAProcessing.class);

	private int loadForceScore = 0;  // Default: < 5kg
    private int couplingScore = 0;   // Default: Good grip
    private int activityScore = 1;   // Default: Assume repetitive/dynamic task part

    // --- REBA Tables (VERIFY THESE VALUES AGAINST OFFICIAL REBA WORKSHEET!) ---
    // Table A: Lookup using Trunk, Neck, Leg Scores
    // Indices: [Trunk Score - 1][Neck Score - 1][Leg Score - 1] (Example Structure - VERIFY EXACTLY)
    // This structure assumes a 3D array representation matching common digital REBA tools.
    // Dimensions: Trunk(1-5+), Neck(1-3), Leg(1-4) -> Verify max scores needed
    private static final int[][][] TABLE_A = {
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
        // Verify this table structure and values meticulously!
    };

    // Table B: Lookup using Upper Arm, Lower Arm, Wrist Scores
    // Indices: [Upper Arm Score - 1][Lower Arm Score - 1][Wrist Score - 1] (Example Structure - VERIFY EXACTLY)
    // Dimensions: UArm(1-6), LArm(1-2), Wrist(1-4) -> Verify max scores
    private static final int[][][] TABLE_B = {
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
        // Verify this table structure and values meticulously!
    };


    // Table C: Lookup using Score A and Score B
    // Indices: [Score A - 1][Score B - 1]
    // Dimensions: ScoreA(1-12), ScoreB(1-12) -> Verify max scores
    private static final int[][] TABLE_C = {
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
          // Verify dimensions and ALL values
    };


    /**
     * Calculates the REBA score based on sensor readings and provided task context.
     * @param s SensorReading object containing joint angles.
     * @param loadKg Load handled in kilograms.
     * @param couplingScore Score from REBA Coupling assessment (0-3).
     * @param activityScore Score from REBA Activity assessment (0 or 1).
     * @return RebaScore object containing detailed results.
     */
    public RebaScore calculateScore(SensorReading s, double loadKg, int couplingScore, int activityScore) {
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
            score.loadForceScore = calculateLoadForceScore(loadKg, false); // Assume no shock force for now
            score.calculationDetails.put("load_kg", loadKg);

            // --- Step 6: Score A ---
            int scoreA = score.groupAScore + score.loadForceScore;
            score.calculationDetails.put("score_A_total", scoreA);

            // --- Step 7-9: Upper Arm, Lower Arm, Wrist Scores ---
            // Assess both sides, use max score for Table B lookup
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
            // NOTE: This is an input here, as duration/frequency cannot be reliably
            // determined from a single SensorReading.
            score.activityScore = activityScore;
            score.calculationDetails.put("activity_score_input", activityScore);

            // --- Step 15: Final Score & Risk ---
            score.finalRebaScore = score.tableCScore + score.activityScore; // Activity added AFTER Table C lookup
            score.riskLevel = getRiskLabel(score.finalRebaScore);

        } catch (Exception e) {
            logger.error("Error calculating REBA score for thingId {}: {}", s.getThingid(), e.getMessage(), e);
            score.riskLevel = "Calculation Error";
            score.finalRebaScore = -1;
        }

        return score;
    }

    // --- Detailed Scoring Functions ---

    private int calculateTrunkScore(SensorReading s, Map<String, Object> details) {
        double tilt = s.getTorsoTilt(); // Flexion (+), Extension (-)
        int score;
        // ** VERIFY REBA RANGES **
        if (Math.abs(tilt) <= 5) score = 1; // Upright 0 +-5 degrees
        else if ((tilt > 5 && tilt <= 20) || (tilt < -5 && tilt >= -20)) score = 2; // Flexion OR Extension 5-20
        else if ((tilt > 20 && tilt <= 60) || tilt < -20) score = 3; // Flexion 20-60 OR Extension > 20
        else score = 4; // Flexion > 60

        details.put("trunk_posture_score", score);

        // Adjustment for Twist / Side Bending
        // ** VERIFY REBA RULES ** Needs TorsoTorsion, TorsoSideTilt
        if (isTrunkTwisted(s.getBackTorsion()) || isTrunkSideBent(s.getTorsoSideTilt())) {
            score += 1;
            details.put("trunk_adjust", 1);
        } else {
            details.put("trunk_adjust", 0);
        }
        return score;
    }

    private int calculateNeckScore(SensorReading s, Map<String, Object> details) {
        double flexExt = s.getNeckFlexExt(); // Flexion (+), Extension (-)
        int score;
         // ** VERIFY REBA RANGES **
        if (flexExt >= 0 && flexExt <= 20) score = 1; // 0-20 Flexion
        else score = 2; // >20 Flexion OR any Extension

        details.put("neck_posture_score", score);

        // Adjustment for Twist / Side Bending
        // ** VERIFY REBA RULES ** Needs NeckTorsion, HeadTilt (as side bend proxy?)
        if (isNeckTwisted(s.getNeckTorsion()) || isNeckSideBent(s.getHeadTilt())) {
            score += 1;
             details.put("neck_adjust", 1);
        } else {
             details.put("neck_adjust", 0);
        }
        return score;
    }

     private int calculateLegScore(SensorReading s, Map<String, Object> details) {
         // ** NEEDS VERIFICATION - Assumes bilateral standing **
         // REBA requires info on seated, unilateral support etc. Defaulting to 1.
         int score = 1; // Base for bilateral, stable support
         details.put("leg_base_support_score", score);

         // Adjustment for Knee Bend (Use average or max of L/R?) - Let's use max bend.
         double kneeFlexL = s.getKneeFlexExtLeft();
         double kneeFlexR = s.getKneeFlexExtRight();
         double maxKneeFlex = Math.max(Math.abs(kneeFlexL), Math.abs(kneeFlexR)); // Consider only flexion magnitude
         int adjustment = 0;
         if (maxKneeFlex > 60) adjustment = 2;
         else if (maxKneeFlex > 30) adjustment = 1;

         details.put("leg_adjust_knee_flex", adjustment);
         return score + adjustment;
     }

    private int calculateUpperArmScore(double flexExt, double abdAdd, double rotation, boolean isSupported, Map<String, Object> details, String side) {
         // ** VERIFY REBA RANGES & RULES **
         int score;
         double absFlex = Math.abs(flexExt);

         if (absFlex <= 20) score = 1;
         else if (absFlex <= 45) score = 2;
         else if (absFlex <= 90) score = 3;
         else score = 4; // > 90

         details.put("upper_arm_" + side + "_flex_score", score);
         int adjustment = 0;
         // Adjust: Shoulder Raised? (Often interpreted as >20deg flex OR >45deg abd - VERIFY)
         if (flexExt < -20) { // REBA specific: Arm in Extension beyond -20 counts as +1 adjustment
             adjustment += 1;
             details.put("upper_arm_" + side + "_adjust_extension", 1);
         }
         // Adjust: Abduction (Check REBA worksheet for specific threshold)
         if (Math.abs(abdAdd) > 45) { // Example threshold - VERIFY
            adjustment += 1;
            details.put("upper_arm_" + side + "_adjust_abd", 1);
         }
          // Adjust: Rotation (Needs specific angle check - VERIFY) - Not implemented here yet
          // if (isRotated(rotation)) adjustment += 1;

         // Adjust: Leaning/Supported
         if (isSupported) {
             adjustment -= 1; // Score reduction for support
             details.put("upper_arm_" + side + "_adjust_support", -1);
         }

         details.put("upper_arm_" + side + "_total_adjust", adjustment);
         return Math.max(1, score + adjustment); // Min score is 1
     }

     private int calculateLowerArmScore(double elbowFlexExt, Map<String, Object> details, String side) {
         // ** VERIFY REBA RANGES **
         int score;
         if (elbowFlexExt >= 60 && elbowFlexExt <= 100) score = 1;
         else score = 2; // <60 OR >100
         details.put("lower_arm_" + side + "_score", score);
         // No adjustments typically
         return score;
     }

     private int calculateWristScore(double flexExt, double radialUlnar, double pronSup, Map<String, Object> details, String side) {
        // ** VERIFY REBA RANGES & RULES **
        int score;
        if (Math.abs(flexExt) <= 15) score = 1;
        else score = 2; // > 15 Flexion or Extension
        details.put("wrist_" + side + "_flex_score", score);

        // Adjustment for Deviation OR Twist
        // Assuming pronSup measures twist from neutral. VERIFY RANGES
        if (Math.abs(radialUlnar) > 10 || Math.abs(pronSup) > 45) { // Example thresholds - VERIFY
            score += 1;
            details.put("wrist_" + side + "_adjust_dev_twist", 1);
        } else {
             details.put("wrist_" + side + "_adjust_dev_twist", 0);
        }
        return score;
     }

     private int calculateLoadForceScore(double loadKg, boolean isShockForce) {
         int score = 0;
         if (loadKg > 10) score = 2;
         else if (loadKg >= 5) score = 1;
         else score = 0;

         if (isShockForce) {
             score += 1;
         }
         return score;
     }

    // --- Table Lookup Helpers (VERIFY LOGIC and CLAMPING!) ---

    private int lookupTableA(int trunkScore, int neckScore, int legScore) {
        // ** THIS IS A COMPLEX LOOKUP - The 3D array structure is one way **
        // ** CONSULT STANDARD REBA WORKSHEET/TOOL for correct mapping **
        // Clamp scores to valid table indices (arrays are 0-based)
        int trunkIdx = Math.max(0, Math.min(TABLE_A.length - 1, trunkScore - 1));
        int neckIdx = Math.max(0, Math.min(TABLE_A[0].length - 1, neckScore - 1));
        int legIdx = Math.max(0, Math.min(TABLE_A[0][0].length - 1, legScore - 1));
        // logger.debug("Table A Lookup: T={}, N={}, L={} -> Idx: T={}, N={}, L={}", trunkScore, neckScore, legScore, trunkIdx, neckIdx, legIdx);
        return TABLE_A[trunkIdx][neckIdx][legIdx];
    }

     private int lookupTableB(int upperArmScore, int lowerArmScore, int wristScore) {
        // ** THIS IS A COMPLEX LOOKUP - The 3D array structure is one way **
        // ** CONSULT STANDARD REBA WORKSHEET/TOOL for correct mapping **
        int uArmIdx = Math.max(0, Math.min(TABLE_B.length - 1, upperArmScore - 1));
        int lArmIdx = Math.max(0, Math.min(TABLE_B[0].length - 1, lowerArmScore - 1));
        int wristIdx = Math.max(0, Math.min(TABLE_B[0][0].length - 1, wristScore - 1));
         // logger.debug("Table B Lookup: UA={}, LA={}, W={} -> Idx: UA={}, LA={}, W={}", upperArmScore, lowerArmScore, wristScore, uArmIdx, lArmIdx, wristIdx);
        return TABLE_B[uArmIdx][lArmIdx][wristIdx];
    }

     private int lookupTableC(int scoreA, int scoreB) {
        int scoreAIdx = Math.max(0, Math.min(TABLE_C.length - 1, scoreA - 1));
        int scoreBIdx = Math.max(0, Math.min(TABLE_C[0].length - 1, scoreB - 1));
        // logger.debug("Table C Lookup: A={}, B={} -> Idx: A={}, B={}", scoreA, scoreB, scoreAIdx, scoreBIdx);
        return TABLE_C[scoreAIdx][scoreBIdx];
    }


    // --- Helper Predicates (Implement based on REBA definitions) ---
    private boolean isTrunkTwisted(double torsoTorsion) {
        // ** VERIFY REBA threshold for twist **
        return Math.abs(torsoTorsion) > 10; // Example
    }
    private boolean isTrunkSideBent(double torsoSideTilt) {
        // ** VERIFY REBA threshold for side bend **
       return Math.abs(torsoSideTilt) > 10; // Example
    }
     private boolean isNeckTwisted(double neckTorsion) {
         // ** VERIFY REBA threshold for twist **
        return Math.abs(neckTorsion) > 15; // Example
    }
    private boolean isNeckSideBent(double headTilt) { // Using headTilt as proxy? Needs verification.
         // ** VERIFY REBA threshold for side bend **
        return Math.abs(headTilt) > 15; // Example
    }
    // ... add other helpers for shoulder raised, wrist deviated etc. ...

    // --- Risk Level Mapping (Based on worksheet image) ---
    private String getRiskLabel(int finalScore) {
        if (finalScore == 1) return "Negligible"; // Score 1
        if (finalScore <= 3) return "Low";      // Score 2-3
        if (finalScore <= 7) return "Medium";   // Score 4-7
        if (finalScore <= 10) return "High";     // Score 8-10
        return "Very High"; // Score 11+
    }

     // --- Optional: Setters for configurable modifiers ---
     public void setLoadForceScore(int score) { this.loadForceScore = Math.max(0, Math.min(3, score)); } // Clamp 0-3? Verify max
     public void setCouplingScore(int score) { this.couplingScore = Math.max(0, Math.min(3, score)); } // Clamp 0-3
     public void setActivityScore(int score) { this.activityScore = Math.max(0, Math.min(1, score)); } // Clamp 0-1
}