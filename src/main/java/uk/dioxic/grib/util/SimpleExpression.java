package uk.dioxic.grib.util;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

class SimpleExpression<TExpression> implements Bson {
    private final String name;
    private final TExpression expression;

    SimpleExpression(final String name, final TExpression expression) {
        this.name = name;
        this.expression = expression;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        writer.writeStartDocument();
        writer.writeName(name);
        BuildersHelper.encodeValue(writer, expression, codecRegistry);
        writer.writeEndDocument();

        return writer.getDocument();
    }
}