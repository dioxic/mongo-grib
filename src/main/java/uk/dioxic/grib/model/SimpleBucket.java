package uk.dioxic.grib.model;

import lombok.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@RequiredArgsConstructor
public class SimpleBucket {
    private static final Logger LOG = LogManager.getLogger(SimpleBucket.class);

    @With
    private final ObjectId id;
    private final Point loc;
    private final LocalDateTime ts;
    private final LocalDateTime calcTs;
    @Singular
    private final Map<String, Float> parameters;

    public SimpleBucket(List<GribRecord> gribRecords) {
        if (gribRecords == null || gribRecords.isEmpty()) {
            throw new IllegalArgumentException("grid records cannot be empty!");
        }

        GribRecord first = gribRecords.get(0);
        loc = first.getLoc();
        ts = first.getTs();
        calcTs = first.getCalcTs();
        parameters = new HashMap<>(gribRecords.size());

        gribRecords.forEach(rec -> {
            if (rec.getLoc() != loc
                    || rec.getTs() != ts
                    || rec.getCalcTs() != calcTs) {
                LOG.error("grib key attributes don't match: {}", gribRecords);
                throw new IllegalArgumentException("grib key attributes don't match!");
            }
            parameters.put(Integer.toString(rec.getParameter()), rec.getValue());
        });

        id = ObjectId.get();

    }
}
