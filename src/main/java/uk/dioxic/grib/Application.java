package uk.dioxic.grib;

import com.mongodb.client.model.geojson.Polygon;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import uk.dioxic.grib.cli.command.FakeCommand;
import uk.dioxic.grib.cli.command.GeneratorCommand;
import uk.dioxic.grib.cli.command.LoadCommand;
import uk.dioxic.grib.cli.command.QueryCommand;
import uk.dioxic.grib.cli.converter.PolygonConverter;
import uk.dioxic.grib.cli.mixin.FormattingMixin;

import java.util.concurrent.Callable;

import static picocli.CommandLine.Mixin;

@Command(name = "benchmark",
        header = "Centrica POC CLI v0.0.1",
        description = "Centrica POC CLI",
        subcommands = {
                HelpCommand.class,
                LoadCommand.class,
                GeneratorCommand.class,
                QueryCommand.class,
                FakeCommand.class
        }
)
public class Application implements Callable<Integer> {

    @Mixin
    private FormattingMixin formattingMixin;

    private static CommandLine cl;

    public static void main(String[] args) {
        cl = new CommandLine(new Application());
        cl.registerConverter(Polygon.class, PolygonConverter::parse);
        cl.setUsageHelpLongOptionsMaxWidth(45);
        System.exit(cl.execute(args));
    }

    @Override
    public Integer call() {
        cl.usage(System.out);
        return 0;
    }

}