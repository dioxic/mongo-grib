package uk.dioxic.grib.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.*;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.jsr310.LocalDateTimeCodec;
import org.bson.types.ObjectId;
import uk.dioxic.grib.model.CalculationBucket;
import uk.dioxic.grib.model.CalculationBucket.CalculationBucketBuilder;

public class CalculationBucketCodec implements CollectibleCodec<CalculationBucket> {

    private final Logger LOG = LogManager.getLogger(this.getClass());
    private final PointCodec pointCodec = new PointCodec();
    private final LocalDateTimeCodec dateCodec = new LocalDateTimeCodec();
    private final ParameterTimestampMapCodec mapCodec = new ParameterTimestampMapCodec();

    @Override
    public CalculationBucket decode(BsonReader reader, DecoderContext decoderContext) {
        CalculationBucketBuilder builder = CalculationBucket.builder();

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
                case "calcTs":
                    builder.calcTs(dateCodec.decode(reader, decoderContext));
                    break;
                case "minTs":
                    builder.minTs(dateCodec.decode(reader, decoderContext));
                    break;
                case "maxTs":
                    builder.maxTs(dateCodec.decode(reader, decoderContext));
                    break;                    
                case "forecasts":
                    reader.readStartArray();
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        builder.forecast(mapCodec.decode(reader, decoderContext));
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
    public void encode(BsonWriter writer, CalculationBucket value, EncoderContext encoderContext) {
        writer.writeStartDocument();

        writer.writeName("loc");
        pointCodec.encode(writer, value.getLocation(), encoderContext);
        writer.writeName("calcTs");
        dateCodec.encode(writer, value.getCalcTs(), encoderContext);
        writer.writeName("maxTs");
        dateCodec.encode(writer, value.getMaxTs(), encoderContext);
        writer.writeName("minTs");
        dateCodec.encode(writer, value.getMinTs(), encoderContext);

        writer.writeStartArray("forecasts");
        value.getForecasts().forEach(forecast -> mapCodec.encode(writer, forecast, encoderContext));
        writer.writeEndArray();

        writer.writeEndDocument();
    }

    @Override
    public Class<CalculationBucket> getEncoderClass() {
        return CalculationBucket.class;
    }

    @Override
    public CalculationBucket generateIdIfAbsentFromDocument(CalculationBucket document) {
        return document.withId(ObjectId.get());
    }

    @Override
    public boolean documentHasId(CalculationBucket document) {
        return document.getId() != null;
    }

    @Override
    public BsonValue getDocumentId(CalculationBucket document) {
        return new BsonObjectId(document.getId());
    }
}
