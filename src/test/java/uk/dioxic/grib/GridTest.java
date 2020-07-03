package uk.dioxic.grib;

import org.junit.jupiter.api.Test;
import uk.dioxic.grib.model.Grid;

import java.util.Arrays;

public class GridTest {

    @Test
    void printGridCounts() {
        Arrays.stream(Grid.values())
                .forEach(grid -> System.out.println(grid.toString() + ": " + grid.getPoints().size()));
    }

}
