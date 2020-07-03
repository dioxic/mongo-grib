package uk.dioxic.grib.cli.mixin;

import lombok.Getter;
import picocli.CommandLine.Option;

import java.time.LocalTime;

public class RollingForecastMixin extends FlattenedForecastMixin {

    @Getter
    @Option(names = {"--deadline"},
            description = "rolling forecast deadline (default: ${DEFAULT-VALUE})",
            defaultValue = "02:00:00",
            paramLabel = "arg")
    private LocalTime deadline;
    
}
