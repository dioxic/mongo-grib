package uk.dioxic.grib.generator;

import reactor.core.publisher.Flux;

public interface Generator<T> {

    Flux<T> generate();

    long recordCount();
}
