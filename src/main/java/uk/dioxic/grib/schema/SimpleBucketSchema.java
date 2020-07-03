package uk.dioxic.grib.schema;

import com.mongodb.client.model.*;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import uk.dioxic.grib.cli.command.SchemaCommand;
import uk.dioxic.grib.codec.SimpleBucketCodec;
import uk.dioxic.grib.csv.Csv;
import uk.dioxic.grib.csv.SimpleBucketCsv;
import uk.dioxic.grib.generator.Generator;
import uk.dioxic.grib.generator.GribGenerator;
import uk.dioxic.grib.model.GribRecord;
import uk.dioxic.grib.model.SimpleBucket;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Accumulators.last;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.compoundIndex;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.ascending;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;

public class SimpleBucketSchema implements Schema<SimpleBucket, GribRecord> {

    @Override
    public void visit(SchemaCommand schemaCommand) {
        schemaCommand.run(this);
    }

    @Override
    public Flux<WriteModel<SimpleBucket>> writeModel(Flux<GribRecord> sourceFlux, Generator<GribRecord> generator) {
        GribGenerator gribGenerator = (GribGenerator) generator;
        return sourceFlux
                .buffer(gribGenerator.getParameters())
                .map(SimpleBucket::new)
                .map(InsertOneModel::new);
    }

    @Override
    public int recordsPerOperation(Generator<GribRecord> generator) {
        GribGenerator gribGenerator = (GribGenerator) generator;
        return gribGenerator.getParameters();
    }

    @Override
    public Mono<String> indexModel(MongoCollection<SimpleBucket> collection) {
        return Mono.from(collection.createIndex(compoundIndex(
                Indexes.ascending("ts"),
                Indexes.ascending("calcTs"),
                Indexes.geo2dsphere("loc")
        ), new IndexOptions().unique(true)));
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
                geoWithin("loc", polygon)));

        Bson project = project(fields(
                include("ts", "calcTs", "loc"),
                include(parameters.stream().map(p -> "parameters." + p).collect(Collectors.toList()))
        ));

        Bson sort = sort(ascending("ts"));

        return List.of(match, sort, project);
    }

    @Override
    public List<Bson> flattenedForecastQuery(LocalDateTime tsMin,
                                             LocalDateTime tsMax,
                                             List<Integer> parameters,
                                             Polygon polygon) {

        Bson match = match(and(
                gte("ts", tsMin),
                lte("ts", tsMax),
                geoWithin("loc", polygon)));

        Bson sort1 = sort(ascending("ts", "calcTs"));

        Bson project1 = project(fields(
                include("ts", "calcTs", "loc"),
                include(parameters.stream().map(p -> "parameters." + p).collect(Collectors.toList()))
        ));

        Document groupBy = new Document()
                .append("ts", "$ts")
                .append("loc", "$loc");

        Bson group = group(groupBy,
                last("calcDate", "$calcDate"),
                last("value", "$value"));

        Bson addFields = Aggregates.addFields(
                new Field<>("parameter", "$_id.parameter"),
                new Field<>("loc", "$_id.loc"),
                new Field<>("ts", "$_id.ts")
        );

        Bson project2 = project(excludeId());

        Bson sort2 = sort(ascending("ts"));

        return List.of(match, sort1, project1, group, addFields, project2, sort2);
    }

    @Override
    public List<Bson> rollingForecastQuery(LocalDateTime tsMin,
                                           LocalDateTime tsMax,
                                           Duration calculationWindow,
                                           List<Integer> parameters,
                                           Polygon polygon) {

        Bson match1 = match(and(
                gt("ts", tsMin),
                lt("ts", tsMax),
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
        return fromCodecs(new SimpleBucketCodec());
    }

    @Override
    public Class<SimpleBucket> getModelClass() {
        return SimpleBucket.class;
    }

    @Override
    public Csv<SimpleBucket> getCsvConverter() {
        return new SimpleBucketCsv();
    }
}
