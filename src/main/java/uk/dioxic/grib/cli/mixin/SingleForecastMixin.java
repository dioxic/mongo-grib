package uk.dioxic.grib.cli.mixin;

import lombok.Getter;
import org.bson.conversions.Bson;
import picocli.CommandLine.Option;
import uk.dioxic.grib.schema.ReadSchema;

import java.time.LocalDateTime;
import java.util.List;

public class SingleForecastMixin extends BaseQueryMixin {

    @Getter
    @Option(names = {"--calculationTime"},
            description = "calculation time in UTC (default: ${DEFAULT-VALUE})",
            defaultValue = "2020-01-01T00:00:00",
            paramLabel = "arg")
    private LocalDateTime calculationTime;

    @Getter
    @Option(names = {"--horizonLimit"},
            description = "forecast horizon limit in hours (default: ${DEFAULT-VALUE})",
            defaultValue = "120",
            paramLabel = "arg")
    private Integer horizonLimit;

    public List<Bson> getQuery(ReadSchema schema) {
        return schema.singleForecastQuery(horizonLimit, calculationTime, getParameters(), getPolygon());
    }

}
