package uk.dioxic.grib.cli.converter;

import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.stream.Collectors;

public class PolygonConverter {

    public static Polygon parse(String polygon) {
        BsonDocument doc = BsonDocument.parse("{ polygon:" + polygon + "}");

        BsonArray points = doc.getArray("polygon");

        return new Polygon(points.stream()
                .map(BsonValue::asArray)
                .map(PolygonConverter::parsePoint)
                .collect(Collectors.toList()));
    }

    private static Position parsePoint(BsonArray point) {
        return new Position(point.get(0).asNumber().doubleValue(), point.get(1).asNumber().doubleValue());
    }
}
