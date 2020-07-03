package uk.dioxic.grib.schema;

import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.codecs.configuration.CodecRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import uk.dioxic.grib.cli.command.SchemaCommand;
import uk.dioxic.grib.csv.Csv;
import uk.dioxic.grib.generator.Generator;

public interface Schema<MODEL,SOURCE> extends ReadSchema {

    void visit(SchemaCommand schemaCommand);

    Flux<WriteModel<MODEL>> writeModel(Flux<SOURCE> sourceFlux, Generator<SOURCE> generator);

    int recordsPerOperation(Generator<SOURCE> generator);

    Mono<String> indexModel(MongoCollection<MODEL> collection);

    CodecRegistry codecRegistry();

    Class<MODEL> getModelClass();

    Csv<MODEL> getCsvConverter();

}
