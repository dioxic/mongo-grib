package uk.dioxic.grib.model;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.With;
import org.bson.types.ObjectId;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
public class CalculationBucket {

    @With
    private final ObjectId id;
    private final Point location;
    private final LocalDateTime calcTs;
    private final LocalDateTime maxTs;
    private final LocalDateTime minTs;

    @Singular
    private final List<ParameterTimestampMap> forecasts;

}
