package uk.dioxic.grib.generator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import reactor.core.publisher.Flux;
import uk.dioxic.grib.model.GribRecord;
import uk.dioxic.grib.model.GribRecord.GribRecordBuilder;
import uk.dioxic.grib.model.Grid;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
public class GribGenerator implements Generator<GribRecord> {

    private final Logger LOG = LogManager.getLogger(this.getClass());

    private final LocalDateTime startDate;
    private final LocalDateTime endDate;
    private final int horizonDays;
    private final int resolutionHours;
    private final int intervalHours;
    private final Grid grid;
    private final int parameters;
    private final int forecasts;
    private final Map<LocalDateTime, List<LocalDateTime>> calculationMap;

    @Builder
    private GribGenerator(int horizonDays, int resolutionHours, int intervalHours, int parameters, @NonNull Grid grid, @NonNull LocalDateTime startDate, @NonNull LocalDateTime endDate) {
        this.startDate = startDate;// != null ? startDate : LocalDateTime.parse("2000-01-01T00:00:00");
        this.endDate = endDate;// != null ? endDate : LocalDateTime.parse("2000-01-02T00:00:00");
        this.horizonDays = horizonDays;
        this.resolutionHours = resolutionHours;
        this.intervalHours = intervalHours;
        this.grid = grid;
        this.parameters = parameters;

        int hours = (int)(endDate.toEpochSecond(ZoneOffset.UTC) - startDate.toEpochSecond(ZoneOffset.UTC)) * 60 * 60;
        this.forecasts = (int)(((double)hours / 24) * (24/intervalHours));

        calculationMap = new HashMap<>();
        for (LocalDateTime calcTs = this.startDate; calcTs.isBefore(this.endDate); calcTs = calcTs.plusHours(intervalHours)) {
            for (LocalDateTime ts = calcTs.plusHours(resolutionHours); ts.isBefore(calcTs.plusDays(horizonDays)); ts = ts.plusHours(resolutionHours)) {
                calculationMap
                        .computeIfAbsent(ts, k -> new ArrayList<>())
                        .add(calcTs);
            }
        }
    }

    public long recordCount() {
        return (long) grid.getPoints().size() * (long) parameters * calculationMap.values()
                .stream()
                .mapToLong(List::size)
                .sum();
    }

    @Override
    public Flux<GribRecord> generate() {
        return generateTsOrder();
    }

    /**
     * Generate raw GRIB record.
     * <p>
     * Output is ordered by calculationTs -> timestamp -> location -> parameter
     * </p>
     * @return GribRecord flux
     */
    private Flux<GribRecord> generateCalcOrder() {
        Random rnd = ThreadLocalRandom.current();
        GribRecordBuilder builder = GribRecord.builder();

        LOG.info("creating grib flux for {} records", recordCount());

        return Flux.generate(
                () -> new CalcState(0, 0, startDate, startDate, startDate.plusDays(horizonDays), -1),
                (state, sink) -> {
                    state.parameter++;
                    if (state.parameter >= parameters) {
                        state.parameter = 0;
                        state.locIdx++;

                        if (state.locIdx >= grid.getPoints().size()) {
                            state.locIdx = 0;
                            state.ts = state.ts.plusHours(resolutionHours);
                            if (state.ts.isEqual(state.forcastEnd)) {
                                state.calcTs = state.calcTs.plusHours(intervalHours);
                                state.ts = state.calcTs;
                                state.forcastEnd = state.calcTs.plusDays(horizonDays);
                                state.forecast++;
                                if (state.forecast >= forecasts) {
                                    sink.complete();
                                }
                            }
                        }
                    }
                    sink.next(builder
                            .id(ObjectId.get())
                            .loc(grid.getPoints().get(state.locIdx))
                            .calcTs(state.calcTs)
                            .ts(state.ts)
                            .parameter(state.parameter)
                            .value(rnd.nextFloat())
                            .build());
                    return state;
                }
        );
    }

    /**
     * Generate raw GRIB record.
     * <p>
     * Output is ordered by timestamp -> location -> calculationTs -> parameter
     * </p>
     * @return GribRecord flux
     */
    @Deprecated
    private Flux<GribRecord> generateTsOrderOld() {
        Random rnd = ThreadLocalRandom.current();
        GribRecordBuilder builder = GribRecord.builder();

        LOG.info("creating grib flux for {} records in TS order", recordCount());

        int calcPerBucket = (horizonDays * 24) / intervalHours;
        List<LocalDateTime> calcDates = IntStream.range(0, forecasts)
                .mapToObj(i -> startDate.minusDays(horizonDays).plusHours(intervalHours * i))
                .collect(Collectors.toList());

        return Flux.generate(
                () -> new TsState2(0, 0, startDate, startDate.minusDays(horizonDays), calcDates, 0, -1),
                (state, sink) -> {
                    state.parameter++;
                    if (state.parameter >= parameters) {
                        state.parameter = 0;
                        state.calcIdx++;
                        state.calcTs = state.calcTs.plusHours(intervalHours);

                        if (!state.calcTs.isBefore(state.ts)) {
                            state.calcIdx = 0;
                            state.locIdx++;

                            if (state.locIdx >= grid.getPoints().size()) {
                                state.locIdx = 0;
                                state.tsIdx++;
                                state.ts = state.ts.plusHours(resolutionHours);

                                if (state.tsIdx >= (intervalHours * horizonDays)) {
                                    sink.complete();
                                }
                            }

                            LocalDateTime deadline = state.ts.minusDays(horizonDays);

                            state.calcDates = state.calcDates.stream()
                                    .filter(calcDate -> calcDate.isAfter(deadline) || calcDate.isEqual(deadline))
                                    .collect(Collectors.toList());

                            state.calcTs = state.calcDates.get(0);
                        }
                    }
                    sink.next(builder
                            .id(ObjectId.get())
                            .loc(grid.getPoints().get(state.locIdx))
                            .calcTs(state.calcTs)
                            .ts(state.ts)
                            .parameter(state.parameter)
                            .value(rnd.nextFloat())
                            .build());
                    return state;
                }
        );
    }

    /**
     * Generate raw GRIB record.
     * <p>
     * Output is ordered by timestamp -> location -> calculationTs -> parameter
     * </p>
     * @return GribRecord flux
     */
    public Flux<GribRecord> generateTsOrder() {
        Random rnd = ThreadLocalRandom.current();
        GribRecordBuilder builder = GribRecord.builder();

        LOG.info("creating grib flux for {} records in TS order", recordCount());

        List<LocalDateTime> tsList = calculationMap
                .keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList());

        return Flux.generate(
                () -> new TsState(calculationMap.get(tsList.get(0))),
                (state, sink) -> {
                    state.parameterIdx++;
                    if (state.parameterIdx >= parameters) {
                        state.parameterIdx = 0;
                        state.calcIdx++;

                        if (state.calcIdx >= state.calculations.size()) {
                            state.calcIdx = 0;
                            state.locIdx++;

                            if (state.locIdx >= grid.getPoints().size()) {
                                state.locIdx = 0;
                                state.tsIdx++;

                                if (state.tsIdx >= tsList.size()) {
                                    sink.complete();
                                    return state;
                                } else {
                                    state.calculations = calculationMap.get(tsList.get(state.tsIdx));
                                }
                            }
                        }
                    }
                    sink.next(builder
                            .id(ObjectId.get())
                            .loc(grid.getPoints().get(state.locIdx))
                            .calcTs(state.calculations.get(state.calcIdx))
                            .ts(tsList.get(state.tsIdx))
                            .parameter(state.parameterIdx)
                            .value(rnd.nextFloat())
                            .build());
                    return state;
                }
        );
    }

    @AllArgsConstructor
    static class CalcState {
        int forecast;
        int locIdx;
        LocalDateTime ts;
        LocalDateTime calcTs;
        LocalDateTime forcastEnd;
        int parameter;
    }

    static class TsState {
        int tsIdx;
        int locIdx;
        int calcIdx;
        int parameterIdx;
        List<LocalDateTime> calculations;

        public TsState(List<LocalDateTime> calculations) {
            this.calculations = calculations;
        }
    }

    @AllArgsConstructor
    static class TsState2 {
        int tsIdx;
        int locIdx;
        LocalDateTime ts;
        LocalDateTime calcTs;
        List<LocalDateTime> calcDates;
        int calcIdx;
        int parameter;
    }

}
