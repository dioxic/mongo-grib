package uk.dioxic.grib.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import uk.dioxic.grib.model.Point;

public class PointCodec implements Codec<Point> {
    @Override
    public Point decode(BsonReader reader, DecoderContext decoderContext) {
        reader.readStartArray();
        double longitude = reader.readDouble();
        double latitude = reader.readDouble();
        reader.readEndArray();
        return new Point(longitude, latitude);
    }

    @Override
    public void encode(BsonWriter writer, Point value, EncoderContext encoderContext) {
        writer.writeStartArray();
        writer.writeDouble(value.getLongitude());
        writer.writeDouble(value.getLatitude());
        writer.writeEndArray();
    }

    @Override
    public Class<Point> getEncoderClass() {
        return Point.class;
    }
}
