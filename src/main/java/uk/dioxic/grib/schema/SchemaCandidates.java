package uk.dioxic.grib.schema;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum SchemaCandidates {
    FLAT(new FlatSchema()),
    SIMPLE_BUCKET(new SimpleBucketSchema()),
    TS_BUCKET(new TimestampBucketSchema(false)),
    TS_BUCKET_UPSERT(new TimestampBucketSchema(true)),
    CALC_BUCKET(new CalculationBucketSchema());

    private final Schema<?,?> schema;

}
