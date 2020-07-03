package uk.dioxic.grib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.test.StepVerifier;
import uk.dioxic.grib.generator.GribGenerator;
import uk.dioxic.grib.model.Grid;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GribGeneratorTest {

    Logger LOG = LogManager.getLogger(this.getClass());

    @ParameterizedTest
    @CsvSource({"5,1,6,3,ONE_POINT", "5,1,6,8,EUROPE", "5,1,6,8,ONE_POINT"})
    public void generateFluxTest(int horizon, int resolution, int interval, int parameters, Grid grid) {
        GribGenerator generator = GribGenerator.builder()
                .startDate(LocalDateTime.parse("2020-01-01T00:00:00"))
                .endDate(LocalDateTime.parse("2020-01-02T00:00:00"))
                .horizonDays(horizon)
                .resolutionHours(resolution)
                .intervalHours(interval)
                .parameters(parameters)
//                .forecasts(forecasts)
                .grid(grid)
                .build();

//        generator.generate().subscribe(System.out::println);

        StepVerifier.create(generator.generate())
                .expectSubscription()
                .expectNextCount(generator.recordCount())
                .verifyComplete();

    }

    @Test
    void quick() {
        LocalDateTime startDate = LocalDateTime.parse("2020-01-01T00:00:00");
        int horizonDays = 2;
        int resolutionHours = 1;
        int intervalHours = 6;
        int forecasts = 1;
        LocalDateTime calcStartDate = startDate.minusDays(horizonDays);

        Map<LocalDateTime, List<LocalDateTime>> tsCalculationMap = new HashMap<>();

        LocalDateTime endDate = startDate.plusHours((horizonDays*24) + (forecasts * intervalHours));

        for (LocalDateTime calcTs = startDate; calcTs.isBefore(endDate); calcTs = calcTs.plusHours(intervalHours)) {
            for (LocalDateTime ts = calcTs.plusHours(resolutionHours); ts.isBefore(calcTs.plusDays(horizonDays)); ts = ts.plusHours(resolutionHours)) {
                tsCalculationMap
                        .computeIfAbsent(ts, k -> new ArrayList<>())
                        .add(calcTs);
            }
        }

        tsCalculationMap
                .keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList())
                .forEach(ts -> System.out.println("ts: " + ts + " calcTs: " + tsCalculationMap.get(ts)));


//        System.out.println(endDate);

    }

}
