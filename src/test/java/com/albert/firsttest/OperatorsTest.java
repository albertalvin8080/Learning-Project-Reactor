package com.albert.firsttest;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class OperatorsTest {

    @Test
    void Flux_SubscribeOn() {
        final Flux<Integer> flux = Flux.range(1, 5)
                .map(i -> {
                    log.info("map-1: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                })
                // subscribeOn(...) makes the entirety of the flux to be processed by parallel threads.
                .subscribeOn(Schedulers.boundedElastic()) // Acts on the Subscriber
                .map(i -> {
                    log.info("map-2: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                });

        flux.subscribe();
    }

    @Test
    void Flux_PublishOn() {
        final Flux<Integer> flux = Flux.range(1, 5)
                .map(i -> {
                    log.info("map-1: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                })
                // publishOn(...) makes only the below operations (downstream operations) to be processed by parallel threads.
                .publishOn(Schedulers.boundedElastic()) // Acts on the Publisher
                .map(i -> {
                    log.info("map-2: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                });

        flux.subscribe();
    }
}
