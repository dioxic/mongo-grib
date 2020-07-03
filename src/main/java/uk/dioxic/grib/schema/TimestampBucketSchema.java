package uk.dioxic.grib.schema;

import com.mongodb.client.model.*;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;
import com.mongodb.reactivestreams.client.MongoCollection;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;
import uk.dioxic.grib.cli.command.SchemaCommand;
import uk.dioxic.grib.codec.Float32BitCodec;
import uk.dioxic.grib.codec.ParameterTimestampMapCodec;
import uk.dioxic.grib.codec.PointCodec;
import uk.dioxic.grib.codec.TimestampBucketCodec;
import uk.dioxic.grib.csv.Csv;
import uk.dioxic.grib.csv.TimestampBucketCsv;
import uk.dioxic.grib.generator.Generator;
import uk.dioxic.grib.generator.GribGenerator;
import uk.dioxic.grib.model.GribRecord;
import uk.dioxic.grib.model.ParameterTimestampMap;
import uk.dioxic.grib.model.TimestampBucket;
import uk.dioxic.grib.util.ProjectionOperators;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Accumulators.last;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.compoundIndex;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.*;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;

@RequiredArgsConstructor
public class TimestampBucketSchema implements Schema<TimestampBucket, GribRecord> {

    private final boolean upsert;

    @Override
    public void visit(SchemaCommand schemaCommand) {
        schemaCommand.run(this);
    }

    @Override
    public Flux<WriteModel<TimestampBucket>> writeModel(Flux<GribRecord> sourceFlux, Generator<GribRecord> generator) {
        return upsert ? upsertWriteModel(sourceFlux) : insertWriteModel(sourceFlux);
    }

    private Flux<WriteModel<TimestampBucket>> insertWriteModel(Flux<GribRecord> sourceFlux) {
        return sourceFlux
                .bufferUntilChanged(grib -> Tuples.of(grib.getTs(), grib.getLoc()))
                .map(TimestampBucket::new)
                .map(InsertOneModel::new);
    }

    private Flux<WriteModel<TimestampBucket>> upsertWriteModel(Flux<GribRecord> sourceFlux) {
        return sourceFlux
                .bufferUntilChanged(grib -> Tuples.of(grib.getTs(), grib.getLoc(), grib.getCalcTs()))
                .map(this::updateModel);
    }

    private int gribsPerDocument(GribGenerator gribGenerator) {
        return (gribGenerator.getParameters() * gribGenerator.getHorizonDays() * 24) / gribGenerator.getIntervalHours();
    }

    @Override
    public int recordsPerOperation(Generator<GribRecord> generator) {
        GribGenerator gribGenerator = (GribGenerator) generator;
        int gribsPerDoc = (gribGenerator.getParameters() * gribGenerator.getHorizonDays() * 24) / gribGenerator.getIntervalHours();
        int calcsPerDoc = (24 * gribGenerator.getHorizonDays()) / gribGenerator.getIntervalHours();

        return upsert ? gribsPerDoc / calcsPerDoc : gribsPerDoc;
    }

    /**
     * Write model if forecast records are not in calculateTs order
     */
    private UpdateOneModel<TimestampBucket> updateModel(List<GribRecord> gribRecords) {
        if (gribRecords == null || gribRecords.isEmpty()) {
            throw new IllegalArgumentException("grid records cannot be empty!");
        }

        GribRecord first = gribRecords.get(0);
        Bson filter = and(
                eq("ts", first.getTs()),
                geoIntersects("loc", new Point(new Position(first.getLoc().getLongitude(), first.getLoc().getLatitude())))
        );

        Map<String, Float> parameters = gribRecords.stream()
                .collect(Collectors.toMap(rec -> Integer.toString(rec.getParameter()), GribRecord::getValue));

        Bson update = combine(
                push("calcs", new ParameterTimestampMap(first.getCalcTs(), parameters)),
                setOnInsert("loc", first.getLoc())
        );

        return new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));
    }

    @Override
    public Mono<String> indexModel(MongoCollection<TimestampBucket> collection) {
        return Mono.from(collection.createIndex(compoundIndex(
                Indexes.ascending("ts"),
                Indexes.geo2dsphere("loc")
        ), new IndexOptions().name("geo").unique(true)));
//                .then(Mono.from(collection.createIndex(compoundIndex(
//                        Indexes.ascending("ts"),
//                        Indexes.ascending("loc.0"),
//                        Indexes.ascending("loc.1")
//                ), new IndexOptions().name("upd").unique(true))));
    }

    @Override
    public List<Bson> singleForecastQuery(int horizonLimit,
                                          LocalDateTime calcTs,
                                          List<Integer> parameters,
                                          Polygon polygon) {

        Bson match = match(and(
                gte("ts", calcTs),
                lte("ts", calcTs.plusHours(horizonLimit)),
                geoWithin("loc", polygon)));

        Bson sort = sort(ascending("ts"));

        Bson filter = project(fields(
                include("ts", "loc"),
                computed("calc", ProjectionOperators.arrayElemAt(ProjectionOperators.filterEq("calcs", "ts", calcTs), 0)),
                include(parameters.stream().map(p -> "parameters." + p).collect(Collectors.toList()))
        ));

        Bson project = project(fields(
                include("ts", "loc"),
                computed("calcTs", "$calc.ts"),
                include(parameters.stream().map(p -> "parameters." + p).collect(Collectors.toList())),
                computed("parameters", parameters.stream()
                        .collect(Collectors.toMap(p -> Integer.toString(p), p -> "$calc." + p))
                )
        ));

        return List.of(match, sort, filter, project);
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

        Bson addFields = addFields(
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
        return fromCodecs(new TimestampBucketCodec(), new ParameterTimestampMapCodec(), new PointCodec(), new Float32BitCodec());
    }

    @Override
    public Class<TimestampBucket> getModelClass() {
        return TimestampBucket.class;
    }

    @Override
    public Csv<TimestampBucket> getCsvConverter() {
        return new TimestampBucketCsv();
    }
}
