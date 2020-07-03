package uk.dioxic.grib.cli.command;

import com.mongodb.reactivestreams.client.MongoCollection;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;
import reactor.core.publisher.Mono;
import uk.dioxic.grib.cli.mixin.*;
import uk.dioxic.grib.loader.LoadRunner;
import uk.dioxic.grib.model.GribRecord;
import uk.dioxic.grib.schema.Schema;

@Command(name = "load",
        description = "loads GRIB records into MongoDB using various schema models",
        subcommands = {HelpCommand.class})
public class LoadCommand implements Runnable, SchemaCommand {

    @Spec
    CommandSpec spec;

    @Mixin
    FormattingMixin formattingMixin;

    @Mixin
    GeneratorMixin generatorMixin;

    @Mixin
    MongoMixin mongoMixin;

    @Mixin
    DataLoadMixin dataLoadMixin;

    @Mixin
    SchemaMixin schemaMixin;

    @Override
    public <MODEL> void run(Schema<MODEL, GribRecord> schema) {

        long start = System.currentTimeMillis();

        mongoMixin.addCodecRegistry(schema.codecRegistry());
        MongoCollection<MODEL> collection = mongoMixin.getCollection(schema.getModelClass());

        LoadRunner<MODEL, GribRecord> loadRunner = LoadRunner.<MODEL, GribRecord>builder()
                .generator(generatorMixin.getGenerator())
                .batchSize(dataLoadMixin.getBatchSize())
                .collection(collection)
                .schema(schema)
                .concurrency(dataLoadMixin.getConcurrency())
                .build();

        if (dataLoadMixin.isDrop()) {
            Mono.from(collection.drop()).block();
        }

        schema.indexModel(collection).block();

        loadRunner.load().block();

        long time = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Loaded " + generatorMixin.getGenerator().recordCount() + " grib records in " + time + "s");
    }

    @Override
    public void run() {
        schemaMixin.getSchema().visit(this);
    }

}