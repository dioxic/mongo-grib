package uk.dioxic.grib.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class Float32BitCodec implements Codec<Float> {

    @Override
    public void encode(final BsonWriter writer, final Float value, final EncoderContext encoderContext) {
        writer.writeInt32(Float.floatToIntBits(value));
    }

    @Override
    public Float decode(final BsonReader reader, final DecoderContext decoderContext) {
        return Float.intBitsToFloat(reader.readInt32());
    }

    @Override
    public Class<Float> getEncoderClass() {
        return Float.class;
    }
}