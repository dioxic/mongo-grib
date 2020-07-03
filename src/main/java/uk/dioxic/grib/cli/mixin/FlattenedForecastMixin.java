package uk.dioxic.grib.cli.mixin;

import lombok.Getter;
import picocli.CommandLine.Option;

import java.time.LocalDateTime;

public class FlattenedForecastMixin extends BaseQueryMixin {
    
    @Getter
    @Option(names = {"--timestampMax"},
            description = "upper bound for timestamp (default: ${DEFAULT-VALUE})",
            defaultValue = "2020-02-01T00:00:00",
            paramLabel = "arg")
    private LocalDateTime timestampMax;

    @Getter
    @Option(names = {"--timestampMin"},
            description = "upper bound for timestamp (default: ${DEFAULT-VALUE})",
            defaultValue = "2020-01-01T00:00:00",
            paramLabel = "arg")
    private LocalDateTime timestampMin;
    
}
