package uk.dioxic.grib.model;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Singular;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@RequiredArgsConstructor
public class ParameterTimestampMap {
    private final LocalDateTime ts;
    @Singular
    private final Map<String, Float> parameters;
}
