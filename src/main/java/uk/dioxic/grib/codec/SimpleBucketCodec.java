package uk.dioxic.grib.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.*;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.jsr310.LocalDateTimeCodec;
import org.bson.types.ObjectId;
import uk.dioxic.grib.model.SimpleBucket;
import uk.dioxic.grib.model.SimpleBucket.SimpleBucketBuilder;

public class SimpleBucketCodec implements CollectibleCodec<SimpleBucket> {

    private final Logger LOG = LogManager.getLogger(this.getClass());
    private final PointCodec pointCodec = new PointCodec();
    private final LocalDateTimeCodec dateCodec = new LocalDateTimeCodec();
    private final Float32BitCodec float32BitCodec = new Float32BitCodec();

    @Override
    public SimpleBucket decode(BsonReader reader, DecoderContext decoderContext) {
        SimpleBucketBuilder builder = SimpleBucket.builder();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();

            switch (fieldName) {
                case "_id":
                    builder.id(reader.readObjectId());
                    break;
                case "loc":
                    builder.loc(pointCodec.decode(reader, decoderContext));
                    break;
                case "ts":
                    builder.ts(dateCodec.decode(reader, decoderContext));
                    break;
                case "calcTs":
                    builder.calcTs(dateCodec.decode(reader, decoderContext));
                    break;
                case "parameters":
                    reader.readStartDocument();
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        builder.parameter(reader.readName(), float32BitCodec.decode(reader, decoderContext));
                    }
                    reader.readEndDocument();
                    break;
                default:
                    LOG.warn("unexpected field {} found in document", fieldName);
            }
        }
        reader.readEndDocument();

        return builder.build();
    }

    @Override
    public void encode(BsonWriter writer, SimpleBucket value, EncoderContext encoderContext) {
        writer.writeStartDocument();

        writer.writeName("loc");
        pointCodec.encode(writer, value.getLoc(), encoderContext);
        writer.writeName("ts");
        dateCodec.encode(writer, value.getTs(), encoderContext);
        writer.writeName("calcTs");
        dateCodec.encode(writer, value.getCalcTs(), encoderContext);

        writer.writeStartDocument("parameters");
        value.getParameters().forEach((k, v) -> {
            writer.writeName(k);
            float32BitCodec.encode(writer, v, encoderContext);
        });
        writer.writeEndDocument();

        writer.writeEndDocument();
    }

    @Override
    public Class<SimpleBucket> getEncoderClass() {
        return SimpleBucket.class;
    }

    @Override
    public SimpleBucket generateIdIfAbsentFromDocument(SimpleBucket document) {
        return document.withId(ObjectId.get());
    }

    @Override
    public boolean documentHasId(SimpleBucket document) {
        return document.getId() != null;
    }

    @Override
    public BsonValue getDocumentId(SimpleBucket document) {
        return new BsonObjectId(document.getId());
    }
}
