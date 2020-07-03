package uk.dioxic.grib.util;

import lombok.RequiredArgsConstructor;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.client.model.Filters.and;

public final class ProjectionOperators {

    public static Bson arrayElemAt(Bson array, int idx) {
        return new ArrayExpression("$arrayElemAt", List.of(array, idx));
    }

    public static Bson filterEq(String input, String field, Object value) {
        return new Document("$filter", and(
                new Document("input", input),
                new Document("as", "v"),
                new Document("cond", new Document("$eq", List.of("$$v." + field, value)))
        ));
    }

    @RequiredArgsConstructor
    static class ArrayExpression implements Bson {

        private final String name;
        private final List<Object> array;

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName(name);
            BuildersHelper.encodeValue(writer, array, codecRegistry);
            writer.writeEndDocument();

            return writer.getDocument();
        }
    }

}
