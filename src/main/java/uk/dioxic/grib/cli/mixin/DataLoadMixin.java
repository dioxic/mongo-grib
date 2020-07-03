package uk.dioxic.grib.cli.mixin;

import lombok.Getter;
import picocli.CommandLine.Option;

@Getter
public class DataLoadMixin {
    @Option(names = {"--drop"},
            description = "drop collection before loading data (default: ${DEFAULT-VALUE})",
            defaultValue = "false")
    private boolean drop;

    @Option(names = {"-b", "--batchSize"},
            description = "insert batch size (default: ${DEFAULT-VALUE})",
            defaultValue = "1000",
            paramLabel = "arg")
    private int batchSize;

    @Option(names = {"--concurrency"},
            description = "reactive flapmap concurrency (default: ${DEFAULT-VALUE})",
            defaultValue = "4",
            paramLabel = "arg")
    private int concurrency;
}
