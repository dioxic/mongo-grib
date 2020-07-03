package uk.dioxic.grib.cli.command;

import uk.dioxic.grib.model.GribRecord;
import uk.dioxic.grib.schema.Schema;

public interface SchemaCommand {

    <MODEL> void run(Schema<MODEL, GribRecord> schema);

}
