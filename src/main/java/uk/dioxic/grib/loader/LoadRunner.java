package uk.dioxic.grib.loader;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.MongoCollection;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import uk.dioxic.grib.generator.Generator;
import uk.dioxic.grib.schema.Schema;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Builder
public class LoadRunner<MODEL, SOURCE> {
    private final Logger LOG = LogManager.getLogger(this.getClass());
    private final Logger METRICS_LOG = LogManager.getLogger("MetricMonitor");
    private final BulkWriteOptions writeOptions = new BulkWriteOptions().ordered(false);

    private final Generator<SOURCE> generator;
    private final MongoCollection<MODEL> collection;
    private final Schema<MODEL, SOURCE> schema;
    private final int concurrency;
    private final int batchSize;

    public Mono<Long> load() {
        Flux<SOURCE> generationFlux = generator.generate();

        Flux<OperationMetrics> results = schema.writeModel(generationFlux, generator)
                .subscribeOn(Schedulers.newSingle("generator"))
                .buffer(batchSize)
                .flatMap(this::bulkWrite, concurrency)
                .share();

        monitor(results);

        return results
                .map(OperationMetrics::getOperationCount)
                .collect(Collectors.summingLong(Long::longValue))
                .doOnNext(count -> LOG.info("total document operations: {}", count));
    }

    private Mono<OperationMetrics> bulkWrite(List<WriteModel<MODEL>> batch) {
        MetricBuilder builder = MetricBuilder.start(batchSize, schema.recordsPerOperation(generator));
        return Mono.from(collection.bulkWrite(batch, writeOptions))
                .map(builder::complete);
    }

    private void monitor(Flux<OperationMetrics> operationMetricsFlux) {
        final AtomicInteger atomicOutputCount = new AtomicInteger();
        final AtomicLong atomicParameterCount = new AtomicLong();

        final AtomicReference<OperationMetrics> atomicMetric = new AtomicReference<>(OperationMetrics.ZERO);
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        final int seconds = 5;

        Runnable monitorTask = () -> {
            if (atomicOutputCount.getAndAccumulate(1, (x, y) -> (x + y) % 10) == 0) {
                METRICS_LOG.info("ops/s\t\t\tparameters/s\t\tlatency (ms)\t\t% complete");
            }
            OperationMetrics windowMetrics = atomicMetric.getAndSet(OperationMetrics.ZERO);
            long totalParameters = atomicParameterCount.addAndGet(windowMetrics.parameterCount);

            METRICS_LOG.info("{}\t\t\t{}\t\t\t{}\t\t\t{}",
                    windowMetrics.getOperationCount() / seconds,
                    windowMetrics.getParameterCount() / seconds,
                    windowMetrics.getAverageLatency(),
                    totalParameters / (generator.recordCount() / 100));
        };

        executor.scheduleAtFixedRate(monitorTask, 1, seconds, TimeUnit.SECONDS);

        operationMetricsFlux
                .doOnComplete(executor::shutdown)
                .doOnError(err -> executor.shutdown())
                .subscribe(metrics -> atomicMetric.accumulateAndGet(metrics, OperationMetrics::add));
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class MetricBuilder {
        private final long startMillis;
        private final int batchSize;
        private final int recordsPerOperation;

        static MetricBuilder start(int batchSize, int recordsPerOperation) {
            return new MetricBuilder(System.currentTimeMillis(), batchSize, recordsPerOperation * batchSize);
        }

        public OperationMetrics complete(BulkWriteResult writeResult) {
            return complete(batchSize);
        }

        public OperationMetrics complete(int operationCount) {
            long duration = System.currentTimeMillis() - startMillis;
            return new OperationMetrics(recordsPerOperation, operationCount, 1, duration);
        }
    }

    @Data
    static class OperationMetrics {
        private final long parameterCount;
        private final long operationCount;
        private final long batchCount;
        private final long duration;

        public OperationMetrics add(OperationMetrics other) {
            return new OperationMetrics(
                    this.parameterCount + other.parameterCount,
                    this.operationCount + other.operationCount,
                    this.batchCount + other.batchCount,
                    this.duration + other.duration);
        }

        public static final OperationMetrics ZERO = new OperationMetrics(0, 0, 0,0);

        public long getAverageLatency() {
            return duration / batchCount;
        }
    }

}
