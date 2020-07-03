package uk.dioxic.grib.cli.command;

import picocli.CommandLine;
import reactor.core.publisher.Mono;
import uk.dioxic.grib.cli.mixin.FormattingMixin;
import uk.dioxic.grib.cli.mixin.MongoMixin;

import java.util.concurrent.Callable;

@CommandLine.Command(name="fake")
public class FakeCommand implements Callable<Integer> {

    @CommandLine.Mixin
    private MongoMixin mongoMixin;

    @CommandLine.Mixin
    private FormattingMixin formattingMixin;



    public void run() {
        System.out.println(mongoMixin.getCollection());
    }

    @Override
    public Integer call() throws Exception {
        System.out.println("h");
        System.out.println(Mono.from(mongoMixin.getCollection().countDocuments()).block());
        return 0;
    }
}
