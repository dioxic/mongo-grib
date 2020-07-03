package uk.dioxic.grib.cli.command;

import picocli.CommandLine.*;
import picocli.CommandLine.Model.CommandSpec;
import uk.dioxic.grib.cli.command.query.SingleForecastCommand;
import uk.dioxic.grib.cli.mixin.FormattingMixin;

@Command(name = "query",
        description = "Query command",
        subcommands = {
                HelpCommand.class,
                SingleForecastCommand.class
        })
public class QueryCommand implements Runnable {

    @Spec
    CommandSpec spec;

    @Mixin
    FormattingMixin formattingMixin;

    @Override
    public void run() {
        throw new ParameterException(spec.commandLine(), "Specify a subcommand");
    }
}