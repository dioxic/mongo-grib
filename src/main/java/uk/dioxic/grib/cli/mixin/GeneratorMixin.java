package uk.dioxic.grib.cli.mixin;

import lombok.Getter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import uk.dioxic.grib.generator.GribGenerator;
import uk.dioxic.grib.model.Grid;

import java.time.LocalDate;

public class GeneratorMixin {

    @Getter
    @ArgGroup(heading = "%nGenerator Options:%n", exclusive = false)
    private final GribOptions gribOptions = new GribOptions();

    private static class GribOptions {
        @Option(names = {"--horizon"},
                description = "forecast horizon in days (default: ${DEFAULT-VALUE})",
                defaultValue = "5",
                paramLabel = "arg")
        private Integer horizon;

        @Option(names = {"--resolution"},
                description = "forecast resolution in hours (default: ${DEFAULT-VALUE})",
                defaultValue = "1",
                paramLabel = "arg")
        private Integer resolution;

        @Option(names = {"--interval"},
                description = "forecast interval in hours (default: ${DEFAULT-VALUE})",
                defaultValue = "6",
                paramLabel = "arg")
        private Integer interval;

        @Option(names = {"--parameters"},
                description = "number of forecast parameters (default: ${DEFAULT-VALUE})",
                defaultValue = "8",
                paramLabel = "arg")
        private Integer parameters;

//        @Option(names = {"-f", "--forecasts"},
//                description = "number of forecasts (default: ${DEFAULT-VALUE})",
//                defaultValue = "1",
//                paramLabel = "arg")
//        private Integer forecasts;

        @Option(names = {"-g", "--grid"},
                description = "coordinate grid, one of ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})",
                defaultValue = "EUROPE",
                paramLabel = "arg")
        private Grid grid;

        @Option(names = {"--start-date"},
                description = "forecast start date (default: ${DEFAULT-VALUE})",
                defaultValue = "2020-01-01",
                paramLabel = "arg")
        private LocalDate startDate;

        @Option(names = {"--end-date"},
                description = "forecast start date (default: ${DEFAULT-VALUE})",
                defaultValue = "2020-01-02",
                paramLabel = "arg")
        private LocalDate endDate;

    }

    public GribGenerator getGenerator() {
        return GribGenerator.builder()
                .horizonDays(gribOptions.horizon)
                .resolutionHours(gribOptions.resolution)
                .intervalHours(gribOptions.interval)
                .parameters(gribOptions.parameters)
                .endDate(gribOptions.endDate.atStartOfDay())
//                .forecasts(gribOptions.forecasts)
                .grid(gribOptions.grid)
                .startDate(gribOptions.startDate.atStartOfDay())
                .build();
    }

}
