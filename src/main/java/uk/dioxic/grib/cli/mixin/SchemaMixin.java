package uk.dioxic.grib.cli.mixin;

import picocli.CommandLine.Option;
import uk.dioxic.grib.schema.Schema;
import uk.dioxic.grib.schema.SchemaCandidates;

public class SchemaMixin {
    @Option(names = {"--schema"},
            description = "schema model, one of ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})",
            defaultValue = "SIMPLE_BUCKET",
            paramLabel = "arg")
    private SchemaCandidates schema;

    public Schema<?, ?> getSchema() {
        return schema.getSchema();
    }
}
