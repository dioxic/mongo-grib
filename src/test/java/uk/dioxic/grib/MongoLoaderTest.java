package uk.dioxic.grib;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Accumulators;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import uk.dioxic.grib.codec.*;
import uk.dioxic.grib.generator.GribGenerator;
import uk.dioxic.grib.loader.LoadRunner;
import uk.dioxic.grib.model.*;
import uk.dioxic.grib.schema.*;

import java.time.LocalDateTime;

import static com.mongodb.client.model.Aggregates.group;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class MongoLoaderTest {

    private static final Logger LOG = LogManager.getLogger(MongoLoaderTest.class);
    private static MongoClient client;
    private GribGenerator generator;

    @BeforeAll
    static void setup() {
        CodecRegistry pocCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromCodecs(new GribRecordCodec(),
                        new SimpleBucketCodec(),
                        new TimestampBucketCodec(),
                        new ParameterTimestampMapCodec(),
                        new CalculationBucketCodec(),
                        new PointCodec()));

        MongoClientSettings mcs = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost:27017"))
                .codecRegistry(pocCodecRegistry)
                .build();

        client = MongoClients.create(mcs);
    }

    @BeforeEach
    void beforeEach() {
        generator = GribGenerator.builder()
                .horizonDays(5)
                .startDate(LocalDateTime.parse("2020-01-01T00:00:00"))
                .endDate(LocalDateTime.parse("2020-01-02T00:00:00"))
                .resolutionHours(1)
                .intervalHours(6)
                .parameters(8)
//                .forecasts(1)
                .grid(Grid.DENMARK)
                .build();
    }

    @Test
    void loadFlatSchema() {

        MongoCollection<GribRecord> collection = client
                .getDatabase("test")
                .getCollection("flatTest", GribRecord.class);

        Mono.from(collection.drop()).block();

        LoadRunner<GribRecord, GribRecord> loader = LoadRunner.<GribRecord, GribRecord>builder()
                .batchSize(100)
                .concurrency(4)
                .collection(collection)
                .schema(new FlatSchema())
                .generator(generator)
                .build();

        Long opsCount = loader.load().block();

        assertThat(Mono.from(collection.countDocuments()).block()).isEqualTo(opsCount);
    }

    @Test
    void loadSimpleBucketSchema() {
        MongoCollection<SimpleBucket> collection = client
                .getDatabase("test")
                .getCollection("simpleBucketTest", SimpleBucket.class);

        Mono.from(collection.drop()).block();

        LoadRunner<SimpleBucket, GribRecord> loader = LoadRunner.<SimpleBucket, GribRecord>builder()
                .batchSize(100)
                .concurrency(4)
                .collection(collection)
                .schema(new SimpleBucketSchema())
                .generator(generator)
                .build();

        loader.load().block();

        assertThat(Mono.from(collection.countDocuments()).block()).isEqualTo(generator.recordCount() / generator.getParameters());
    }

    @Test
    void loadTimestampBucketSchema() {
        MongoCollection<TimestampBucket> collection = client
                .getDatabase("test")
                .getCollection("tsBucketTest", TimestampBucket.class);

        Mono.from(collection.drop()).block();

        Schema<TimestampBucket, GribRecord> schema = new TimestampBucketSchema(true);

        schema.indexModel(collection).block();

        LoadRunner<TimestampBucket, GribRecord> loader = LoadRunner.<TimestampBucket, GribRecord>builder()
                .batchSize(1000)
                .concurrency(4)
                .collection(collection)
                .schema(schema)
                .generator(generator)
                .build();

        loader.load().block();

        Bson countCalc = group(null, Accumulators.sum("count", new Document("$size", "$calcs")));

        MongoCollection<Document> docCollection = client
                .getDatabase("test")
                .getCollection("tsBucketTest");

        Integer count = Mono.from(docCollection.aggregate(singletonList(countCalc)))
                .map(doc -> doc.getInteger("count"))
                .block();

        assertThat(count)
                .isNotNull()
                .isEqualTo(generator.recordCount() / generator.getParameters());
    }

    @Test
    void loadCalculationBucketSchema() {
        MongoCollection<CalculationBucket> collection = client
                .getDatabase("test")
                .getCollection("calcBucketTest", CalculationBucket.class);

        Mono.from(collection.drop()).block();

        Schema<CalculationBucket, GribRecord> schema = new CalculationBucketSchema();

        schema.indexModel(collection).block();

        LoadRunner<CalculationBucket, GribRecord> loader = LoadRunner.<CalculationBucket, GribRecord>builder()
                .batchSize(100)
                .concurrency(4)
                .collection(collection)
                .schema(schema)
                .generator(generator)
                .build();

        loader.load().block();

        Bson countCalc = group(null, Accumulators.sum("count", new Document("$size", "$forecasts")));

        MongoCollection<Document> docCollection = client
                .getDatabase("test")
                .getCollection("calcBucketTest");

        Integer count = Mono.from(docCollection.aggregate(singletonList(countCalc)))
                .map(doc -> doc.getInteger("count"))
                .block();

        assertThat(count)
                .isNotNull()
                .isEqualTo(generator.recordCount() / generator.getParameters());
    }

}
