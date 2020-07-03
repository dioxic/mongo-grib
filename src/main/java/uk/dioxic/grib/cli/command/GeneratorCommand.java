package uk.dioxic.grib.cli.command;

import picocli.CommandLine;
import picocli.CommandLine.*;
import picocli.CommandLine.Model.CommandSpec;
import uk.dioxic.grib.cli.mixin.FormattingMixin;
import uk.dioxic.grib.cli.mixin.GeneratorMixin;
import uk.dioxic.grib.csv.GribRecordCsv;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.concurrent.Callable;

@Command(name = "generate",
        description = "GRIB generator command",
        subcommands = {
                HelpCommand.class,
                GeneratorCommand.DumpCommand.class
        })
public class GeneratorCommand implements Runnable {

    @Spec
    CommandSpec spec;

    @Mixin
    FormattingMixin formattingMixin;

    @Mixin
    GeneratorMixin generatorMixin;

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Specify a subcommand");
    }

    @Command(name = "count",
            description = "counts the number of GRIB records that would be generated")
    public void count() {
        NumberFormat format = NumberFormat.getInstance();
        format.setGroupingUsed(true);
        System.out.println(format.format(generatorMixin.getGenerator().recordCount()));
    }

    @Command(name = "dump",
            description = "dumps generated GRIB records to a CSV file")
    static class DumpCommand implements Callable<Integer> {

        @Mixin
        FormattingMixin formattingMixin;

        @ParentCommand
        GeneratorCommand parent;

        @Option(names = {"-o", "--out"},
                description = "output file",
                required = true,
                paramLabel = "arg")
        private Path file;

        @Override
        public Integer call() {
            GribRecordCsv csv = new GribRecordCsv();
            System.out.println("Dumping records to " + file.getFileName().toString());

            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(file))) {
                parent.generatorMixin.getGenerator().generate()
                        .doOnSubscribe(sub -> writer.println(csv.getHeader(Collections.emptyList())))
                        .map(csv::getLine)
                        .doOnNext(writer::println)
                        .blockLast();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Dump complete");

            return 0;
        }
    }
}
