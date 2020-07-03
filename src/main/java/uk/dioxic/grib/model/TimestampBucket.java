package uk.dioxic.grib.model;

import lombok.*;
import org.bson.types.ObjectId;
import uk.dioxic.grib.model.ParameterTimestampMap.ParameterTimestampMapBuilder;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@RequiredArgsConstructor
public class TimestampBucket {

    @With
    private final ObjectId id;
    private final Point location;
    private final LocalDateTime ts;
    @Singular
    private final List<ParameterTimestampMap> calculations;

    public TimestampBucket(List<GribRecord> gribRecords) {
        if (gribRecords == null || gribRecords.isEmpty()) {
            throw new IllegalArgumentException("grid records cannot be empty!");
        }

        GribRecord first = gribRecords.get(0);
        location = first.getLoc();
        ts = first.getTs();

        LocalDateTime prevCalcTs = null;
        ParameterTimestampMapBuilder parameterMapBuilder = ParameterTimestampMap.builder();
        calculations = new ArrayList<>();

        for (GribRecord gribRecord : gribRecords) {
            if (location != gribRecord.getLoc() ||
                    ts != gribRecord.getTs()) {
                throw new IllegalStateException("location or ts fields don't match in group");
            }

            if (prevCalcTs != null && prevCalcTs != gribRecord.getCalcTs()) {
                calculations.add(parameterMapBuilder.build());
                parameterMapBuilder.clearParameters();
            }
            parameterMapBuilder.ts(gribRecord.getCalcTs());
            parameterMapBuilder.parameter(Integer.toString(gribRecord.getParameter()), gribRecord.getValue());

            prevCalcTs = gribRecord.getCalcTs();
        }

        calculations.add(parameterMapBuilder.build());

        id = ObjectId.get();

    }

}
