package uk.dioxic.grib.schema;

import com.mongodb.client.model.geojson.Polygon;
import org.bson.conversions.Bson;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

public interface ReadSchema {
    List<Bson> singleForecastQuery(
            int horizonLimit,
            LocalDateTime calcTs,
            List<Integer> parameters,
            Polygon polygon);

    List<Bson> flattenedForecastQuery(
            LocalDateTime tsMin,
            LocalDateTime tsMax,
            List<Integer> parameters,
            Polygon polygon);

    List<Bson> rollingForecastQuery(
            LocalDateTime tsMin,
            LocalDateTime tsMax,
            Duration calculationWindow,
            List<Integer> parameters,
            Polygon polygon);
}
