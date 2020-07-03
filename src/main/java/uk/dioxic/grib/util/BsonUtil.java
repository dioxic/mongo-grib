package uk.dioxic.grib.util;

import com.mongodb.MongoClientSettings;
import org.bson.BsonArray;
import org.bson.Document;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;
import java.util.List;
import java.util.stream.Collectors;

public class BsonUtil {

    private static final JsonWriterSettings DEFAULT_JWS = JsonWriterSettings.builder().build();

    public static String toJson(List<Bson> bson) {
        BsonArray bsonArray = new BsonArray(bson.stream()
                .map(b -> b.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()))
                .collect(Collectors.toList()));
        return toJson(bsonArray, DEFAULT_JWS, new BsonArrayCodec(MongoClientSettings.getDefaultCodecRegistry()));
    }

    public static String toJson(BsonArray bson, final JsonWriterSettings writerSettings, final Encoder<BsonArray> encoder) {
        JsonWriter writer = new JsonWriter(new StringWriter(), writerSettings);
        encoder.encode(writer, bson, EncoderContext.builder().build());
        return writer.getWriter().toString();
    }

}
