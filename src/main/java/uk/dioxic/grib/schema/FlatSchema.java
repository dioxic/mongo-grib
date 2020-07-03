package uk.dioxic.grib.schema;

import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import uk.dioxic.grib.cli.command.SchemaCommand;
import uk.dioxic.grib.codec.GribRecordCodec;
import uk.dioxic.grib.csv.Csv;
import uk.dioxic.grib.csv.GribRecordCsv;
import uk.dioxic.grib.generator.Generator;
import uk.dioxic.grib.model.GribRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static com.mongodb.client.model.Accumulators.last;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.ascending;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;

public class FlatSchema implements Schema<GribRecord, GribRecord> {

    @Override
    public void visit(SchemaCommand schemaCommand) {
        schemaCommand.run(this);
    }

    @Override
    public Flux<WriteModel<GribRecord>> writeModel(Flux<GribRecord> sourceFlux, Generator<GribRecord> generator) {
        return sourceFlux
                .map(InsertOneModel::new);
    }

    @Override
    public int recordsPerOperation(Generator<GribRecord> generator) {
        return 1;
    }

    @Override
    public Mono<String> indexModel(MongoCollection<GribRecord> collection) {
        return Mono.from(collection.createIndex(Indexes.compoundIndex(
                Indexes.ascending("ts"),
                Indexes.ascending("calcTs"),
                Indexes.ascending("parameter"),
                Indexes.geo2dsphere("loc")
        )));
    }

    @Override
    public List<Bson> singleForecastQuery(int horizonLimit,
                                          LocalDateTime calcTs,
                                          List<Integer> parameters,
                                          Polygon polygon) {

        Bson match = match(and(
                gte("ts", calcTs),
                lte("ts", calcTs.plusHours(horizonLimit)),
                eq("calcTs", calcTs),
                in("parameter", parameters),
                geoWithin("loc", polygon)));

        Bson sort = sort(ascending("ts"));

        return List.of(match, sort);
    }

    @Override
    public List<Bson> flattenedForecastQuery(LocalDateTime tsMin,
                                             LocalDateTime tsMax,
                                             List<Integer> parameters,
                                             Polygon polygon) {

        Bson match = match(and(
                gte("ts", tsMin),
                lte("ts", tsMax),
                in("parameter", parameters),
                geoWithin("loc", polygon)));

        Bson sort1 = sort(ascending("ts", "calcTs"));

        Document groupBy = new Document()
                .append("ts", "$ts")
                .append("loc", "$loc")
                .append("parameter", "$parameter");

        Bson group = group(groupBy,
                last("calcDate", "$calcDate"),
                last("value", "$value"));

        Bson project = fields(
                excludeId(),
                computed("parameter", "$_id.parameter"),
                computed("loc", "$_id.loc"),
                computed("ts", "$_id.ts"),
                include("calcTs", "value")
        );

        Bson sort2 = sort(ascending("ts"));

        return List.of(match, sort1, group, project, sort2);
    }

    @Override
    public List<Bson> rollingForecastQuery(LocalDateTime tsMin,
                                           LocalDateTime tsMax,
                                           Duration calculationWindow,
                                           List<Integer> parameters,
                                           Polygon polygon) {

        Bson match1 = match(and(
                gte("ts", tsMin),
                lte("ts", tsMax),
                in("parameter", parameters),
                geoWithin("loc", polygon)));

        Bson sort1 = sort(ascending("ts", "calcTs"));

        Bson dateTrunc = new Document("$dateFromParts", and(
                new Document("year", new Document("$year", "$ts")),
                new Document("month", new Document("$month", "$ts")),
                new Document("day", new Document("$dayOfMonth", "$ts")))
        );

        Bson dateSubtract = new Document("$subtract", List.of(dateTrunc, calculationWindow.toMillis()));

        Bson lessThanExpr = new Document("$lt", List.of("$calcTs", dateSubtract));

        Bson match2 = match(expr(lessThanExpr));

        Document groupBy = new Document()
                .append("ts", "$ts")
                .append("loc", "$loc")
                .append("parameter", "$parameter");

        Bson group = group(groupBy,
                last("calcDate", "$calcDate"),
                last("value", "$value"));

        Bson project = fields(
                excludeId(),
                computed("parameter", "$_id.parameter"),
                computed("loc", "$_id.loc"),
                computed("ts", "$_id.ts"),
                include("calcTs", "value")
        );

        Bson sort2 = sort(ascending("ts"));

        return List.of(match1, sort1, match2, group, project, sort2);
    }

    @Override
    public CodecRegistry codecRegistry() {
        return fromCodecs(new GribRecordCodec());
    }

    @Override
    public Class<GribRecord> getModelClass() {
        return GribRecord.class;
    }

    @Override
    public Csv<GribRecord> getCsvConverter() {
        return new GribRecordCsv();
    }
}
