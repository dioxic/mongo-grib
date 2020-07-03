package uk.dioxic.grib.model;

import lombok.Builder;
import lombok.Data;
import lombok.With;
import org.bson.types.ObjectId;

import java.time.LocalDateTime;

@Data
@Builder
public class GribRecord {
    @With
    private final ObjectId id;
    private final Point loc;
    private final LocalDateTime ts;
    private final LocalDateTime calcTs;
    private final int parameter;
    private final float value;

}
