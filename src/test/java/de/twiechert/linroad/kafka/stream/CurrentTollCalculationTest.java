package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.model.CurrentToll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the toll calculation logic from CurrentTollStreamBuilder.
 * Verifies the toll formula and conditions under which tolls are applicable.
 */
class CurrentTollCalculationTest {

    @Test
    void tollNotApplicable_whenAverageVelocityAbove40() {
        // avg velocity >= 40 -> no toll
        CurrentTollStreamBuilder.CurrentTollIntermediate intermediate =
                new CurrentTollStreamBuilder.CurrentTollIntermediate(5L, 45.0, 60);
        assertFalse(intermediate.isTollApplicable(),
                "Toll should not apply when average velocity >= 40");
    }

    @Test
    void tollNotApplicable_whenVehicleCountBelow50() {
        // num vehicles <= 50 -> no toll
        CurrentTollStreamBuilder.CurrentTollIntermediate intermediate =
                new CurrentTollStreamBuilder.CurrentTollIntermediate(5L, 30.0, 50);
        assertFalse(intermediate.isTollApplicable(),
                "Toll should not apply when vehicle count <= 50");
    }

    @Test
    void tollNotApplicable_whenAccidentDetected() {
        CurrentTollStreamBuilder.CurrentTollIntermediate intermediate =
                new CurrentTollStreamBuilder.CurrentTollIntermediate(5L, 30.0, 60, false);
        assertFalse(intermediate.isTollApplicable(),
                "Toll should not apply when accident is detected");
    }

    @Test
    void tollApplicable_whenAllConditionsMet() {
        // avg velocity < 40, num vehicles > 50, no accident
        CurrentTollStreamBuilder.CurrentTollIntermediate intermediate =
                new CurrentTollStreamBuilder.CurrentTollIntermediate(5L, 30.0, 60, true);
        assertTrue(intermediate.isTollApplicable(),
                "Toll should apply: velocity < 40, vehicles > 50, no accident");
    }

    @Test
    void tollFormula_calculatesCorrectly() {
        // toll = 2 * (numVehicles - 50)^2
        // For 60 vehicles: 2 * (60 - 50)^2 = 2 * 100 = 200
        CurrentTollStreamBuilder.CurrentTollIntermediate intermediate =
                new CurrentTollStreamBuilder.CurrentTollIntermediate(5L, 30.0, 60, true);
        CurrentToll toll = intermediate.calculateCurrentToll();

        assertEquals(6L, toll.getMinute(), "Toll minute should be input minute + 1");
        assertEquals(200.0, toll.getToll(), 0.001, "Toll should be 2*(60-50)^2 = 200");
        assertEquals(30.0, toll.getVelocity(), 0.001, "Velocity should be preserved");
    }

    @Test
    void tollFormula_withDifferentVehicleCounts() {
        // 51 vehicles: 2 * (51 - 50)^2 = 2 * 1 = 2
        CurrentTollStreamBuilder.CurrentTollIntermediate intermediate51 =
                new CurrentTollStreamBuilder.CurrentTollIntermediate(5L, 30.0, 51, true);
        assertEquals(2.0, intermediate51.calculateCurrentToll().getToll(), 0.001);

        // 100 vehicles: 2 * (100 - 50)^2 = 2 * 2500 = 5000
        CurrentTollStreamBuilder.CurrentTollIntermediate intermediate100 =
                new CurrentTollStreamBuilder.CurrentTollIntermediate(5L, 30.0, 100, true);
        assertEquals(5000.0, intermediate100.calculateCurrentToll().getToll(), 0.001);
    }

    @Test
    void setNoAccident_updatesCorrectly() {
        CurrentTollStreamBuilder.CurrentTollIntermediate base =
                new CurrentTollStreamBuilder.CurrentTollIntermediate(5L, 30.0, 60, true);
        assertTrue(base.hasNoAccident());

        CurrentTollStreamBuilder.CurrentTollIntermediate withAccident = base.setNoAccident(false);
        assertFalse(withAccident.hasNoAccident());
        assertFalse(withAccident.isTollApplicable());
    }
}
