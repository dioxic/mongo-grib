package uk.dioxic.grib.cli.mixin;

import com.mongodb.client.model.geojson.Polygon;
import lombok.Getter;
import picocli.CommandLine.Option;

import java.util.List;

public class BaseQueryMixin {

    @Getter
    @Option(names = {"--polygon"},
            description = "geospatial polygon, [long,lat] (default: ${DEFAULT-VALUE})",
            defaultValue = "[[8, 54.5], [12.7, 54.5], [12.7, 57.8], [8, 57.8], [8, 54.5]]",
            paramLabel = "arg")
    private Polygon polygon;

    @Getter
    @Option(names = {"--parameters"},
            description = "list of parameters (default: ${DEFAULT-VALUE})",
            defaultValue = "1,2,7",
            split = ",",
            paramLabel = "arg")
    private List<Integer> parameters;
}
