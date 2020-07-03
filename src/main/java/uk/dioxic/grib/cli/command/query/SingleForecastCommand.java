package uk.dioxic.grib.cli.command.query;

import com.mongodb.MongoClientSettings;
import org.bson.Document;
import org.bson.conversions.Bson;
import picocli.CommandLine;
import picocli.CommandLine.*;
import picocli.CommandLine.Model.CommandSpec;
import reactor.core.publisher.Flux;
import uk.dioxic.grib.cli.command.SchemaCommand;
import uk.dioxic.grib.cli.mixin.FormattingMixin;
import uk.dioxic.grib.cli.mixin.MongoMixin;
import uk.dioxic.grib.cli.mixin.SchemaMixin;
import uk.dioxic.grib.cli.mixin.SingleForecastMixin;
import uk.dioxic.grib.csv.Csv;
import uk.dioxic.grib.model.GribRecord;
import uk.dioxic.grib.schema.Schema;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Command(name = "single",
        description = "single forecast query",
        subcommands = {
                HelpCommand.class,
                SingleForecastCommand.DumpCommand.class
        })
public class SingleForecastCommand implements Runnable {

    @Spec
    CommandSpec spec;

    @Mixin
    FormattingMixin formattingMixin;

    @Mixin
    SingleForecastMixin forecastMixin;

    @Mixin
    SchemaMixin schemaMixin;

    @Command(name = "print",
            description = "print query in json format")
    public void print() {
        System.out.println(forecastMixin.getQuery(schemaMixin.getSchema())
                .stream()
                .map(bson -> bson.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()))
                .collect(Collectors.toList()));
    }

    @Command(name = "dump",
            description = "dumps query output to a CSV file")
    static class DumpCommand implements Callable<Integer>, SchemaCommand {

        @Mixin
        FormattingMixin formattingMixin;

        @Mixin
        MongoMixin mongoMixin;

        @ParentCommand
        SingleForecastCommand parent;

        @Option(names = {"-o", "--out"},
                description = "output file",
                required = true,
                paramLabel = "arg")
        private Path file;

        @Override
        public Integer call() {
            System.out.println("Dumping query output to " + file.getFileName().toString());

            parent.schemaMixin.getSchema().visit(this);

            System.out.println("Dump complete");

            return 0;
        }

        public <MODEL> void run(Schema<MODEL, GribRecord> schema) {
            Csv<MODEL> csv = schema.getCsvConverter();
            mongoMixin.addCodecRegistry(schema.codecRegistry());

            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(file))) {
                List<Bson> pipeline = parent.forecastMixin.getQuery(schema);

                Flux.from(mongoMixin.getCollection(schema.getModelClass()).aggregate(pipeline))
                        .doOnSubscribe(sub -> writer.println(csv.getHeader(parent.forecastMixin.getParameters())))
                        .map(csv::getLine)
                        .doOnNext(writer::println)
                        .blockLast();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Specify a subcommand");
    }
}
