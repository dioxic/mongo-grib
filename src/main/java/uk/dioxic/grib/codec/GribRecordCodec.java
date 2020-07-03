package uk.dioxic.grib.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.*;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.jsr310.LocalDateTimeCodec;
import org.bson.types.ObjectId;
import uk.dioxic.grib.model.GribRecord;
import uk.dioxic.grib.model.GribRecord.GribRecordBuilder;

public class GribRecordCodec implements CollectibleCodec<GribRecord> {

    private final Logger LOG = LogManager.getLogger(this.getClass());
    private final PointCodec pointCodec = new PointCodec();
    private final LocalDateTimeCodec dateCodec = new LocalDateTimeCodec();
    private final Float32BitCodec float32BitCodec = new Float32BitCodec();

    @Override
    public GribRecord decode(BsonReader reader, DecoderContext decoderContext) {
        GribRecordBuilder builder = GribRecord.builder();

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
                case "parameter":
                    builder.parameter(reader.readInt32());
                    break;
                case "value":
                    builder.value((float)reader.readDouble());
                    break;
                default:
                    LOG.warn("unexpected field {} found in document", fieldName);
            }
        }
        reader.readEndDocument();

        return builder.build();
    }

    @Override
    public void encode(BsonWriter writer, GribRecord value, EncoderContext encoderContext) {
        writer.writeStartDocument();

        writer.writeName("loc");
        pointCodec.encode(writer, value.getLoc(), encoderContext);
        writer.writeName("ts");
        dateCodec.encode(writer, value.getTs(), encoderContext);
        writer.writeName("calcTs");
        dateCodec.encode(writer, value.getCalcTs(), encoderContext);
        writer.writeInt32("parameter", value.getParameter());
        writer.writeDouble("value", value.getValue());

        writer.writeEndDocument();
    }

    @Override
    public Class<GribRecord> getEncoderClass() {
        return GribRecord.class;
    }

    @Override
    public GribRecord generateIdIfAbsentFromDocument(GribRecord document) {
        return document.withId(ObjectId.get());
    }

    @Override
    public boolean documentHasId(GribRecord document) {
        return document.getId() != null;
    }

    @Override
    public BsonValue getDocumentId(GribRecord document) {
        return new BsonObjectId(document.getId());
    }
}
