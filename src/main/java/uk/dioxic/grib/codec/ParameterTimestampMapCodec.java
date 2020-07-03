package uk.dioxic.grib.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.jsr310.LocalDateTimeCodec;
import uk.dioxic.grib.model.ParameterTimestampMap;
import uk.dioxic.grib.model.ParameterTimestampMap.ParameterTimestampMapBuilder;

public class ParameterTimestampMapCodec implements Codec<ParameterTimestampMap> {

    private final Logger LOG = LogManager.getLogger(this.getClass());
    private final LocalDateTimeCodec dateCodec = new LocalDateTimeCodec();
    private final Float32BitCodec float32BitCodec = new Float32BitCodec();


    @Override
    public ParameterTimestampMap decode(BsonReader reader, DecoderContext decoderContext) {
        ParameterTimestampMapBuilder builder = ParameterTimestampMap.builder();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();

            if (fieldName.equals("ts")) {
                builder.ts(dateCodec.decode(reader, decoderContext));
            }
            else {
                builder.parameter(fieldName, float32BitCodec.decode(reader, decoderContext));
            }
        }
        reader.readEndDocument();

        return builder.build();
    }

    @Override
    public void encode(BsonWriter writer, ParameterTimestampMap value, EncoderContext encoderContext) {
        writer.writeStartDocument();

        writer.writeName("ts");
        dateCodec.encode(writer, value.getTs(), encoderContext);

        value.getParameters().forEach((k, v) -> {
            writer.writeName(k);
            float32BitCodec.encode(writer, v, encoderContext);
        });

        writer.writeEndDocument();
    }

    @Override
    public Class<ParameterTimestampMap> getEncoderClass() {
        return ParameterTimestampMap.class;
    }

}
