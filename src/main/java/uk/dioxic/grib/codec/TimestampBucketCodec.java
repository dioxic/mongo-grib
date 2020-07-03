package uk.dioxic.grib.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.*;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.jsr310.LocalDateTimeCodec;
import org.bson.types.ObjectId;
import uk.dioxic.grib.model.TimestampBucket;
import uk.dioxic.grib.model.TimestampBucket.TimestampBucketBuilder;

public class TimestampBucketCodec implements CollectibleCodec<TimestampBucket> {

    private final Logger LOG = LogManager.getLogger(this.getClass());
    private final PointCodec pointCodec = new PointCodec();
    private final LocalDateTimeCodec dateCodec = new LocalDateTimeCodec();
    private final ParameterTimestampMapCodec mapCodec = new ParameterTimestampMapCodec();

    @Override
    public TimestampBucket decode(BsonReader reader, DecoderContext decoderContext) {
        TimestampBucketBuilder builder = TimestampBucket.builder();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();

            switch (fieldName) {
                case "_id":
                    builder.id(reader.readObjectId());
                    break;
                case "loc":
                    builder.location(pointCodec.decode(reader, decoderContext));
                    break;
                case "ts":
                    builder.ts(dateCodec.decode(reader, decoderContext));
                    break;
                case "calcs":
                    reader.readStartArray();
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        builder.calculation(mapCodec.decode(reader, decoderContext));
                    }
                    reader.readEndArray();
                    break;
                default:
                    LOG.warn("unexpected field {} found in document", fieldName);
            }
        }
        reader.readEndDocument();

        return builder.build();
    }

    @Override
    public void encode(BsonWriter writer, TimestampBucket value, EncoderContext encoderContext) {
        writer.writeStartDocument();

        writer.writeName("loc");
        pointCodec.encode(writer, value.getLocation(), encoderContext);
        writer.writeName("ts");
        dateCodec.encode(writer, value.getTs(), encoderContext);

        writer.writeStartArray("calcs");
        value.getCalculations().forEach(calc -> mapCodec.encode(writer, calc, encoderContext));
        writer.writeEndArray();

        writer.writeEndDocument();
    }

    @Override
    public Class<TimestampBucket> getEncoderClass() {
        return TimestampBucket.class;
    }

    @Override
    public TimestampBucket generateIdIfAbsentFromDocument(TimestampBucket document) {
        return document.withId(ObjectId.get());
    }

    @Override
    public boolean documentHasId(TimestampBucket document) {
        return document.getId() != null;
    }

    @Override
    public BsonValue getDocumentId(TimestampBucket document) {
        return new BsonObjectId(document.getId());
    }
}
