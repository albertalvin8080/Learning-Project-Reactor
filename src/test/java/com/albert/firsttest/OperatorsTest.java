package com.albert.firsttest;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/*
 * Note to self: you used the word 'flux' (uncased) meaning 'stream of data/events' in the comments.
 * */
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

    @Test
    void Flux_Multiple_SubscribeOn() {
        final Flux<Integer> flux = Flux.range(1, 5)
                // The first call to subscribeOn() overlaps subsequent calls.
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("map-1: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                })
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("map-2: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                });

        flux.subscribe();
    }

    @Test
    void Flux_Multiple_PublishOn() {
        /*
         * PublishOn(...) allows the existence of more than one Scheduler to
         * manage the operations in the same flux (Unlike subscribeOn(...))
         * */
        final Flux<Integer> flux = Flux.range(1, 5)
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("map-1: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                })
                // The operations below this call are handled by the scheduler specified here.
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("map-2: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                });

        flux.subscribe();
    }

    @Test
    void Flux_Merging_PublishOnWithSubscribeOn() {
        /*
         * publishOn(...) has precedence over subscribeOn(...), so in this order,
         * the call for subscribeOn(...) doesn't add another Scheduler to manage the flux.
         * */
        final Flux<Integer> flux = Flux.range(1, 5)
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("map-1: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                })
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("map-2: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                });

        flux.subscribe();
    }

    @Test
    void Flux_Merging_SubscribeOnWithPublishOn() {
        /*
         * Starting with subscribeOn(...), the first map() is managed by the Scheduler passed to it,
         * and the second Scheduler, passed to publishOn(...), takes the management of the subsequent map.
         * */
        final Flux<Integer> flux = Flux.range(1, 5)
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("map-1: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                })
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("map-2: {}, {}", Thread.currentThread().getName(), i);
                    return i;
                });

        flux.subscribe();
    }

    @Test
    void Mono_FromCallable_SubscribeOn() throws InterruptedException {
        /*
         * This is the way in which you should communicate with external APIs.
         * */
        final Mono<List<String>> mono = Mono
                .fromCallable(() -> Files.readAllLines(Path.of("my-text.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic()); // recommended by documentation for I/O operations.

        mono.subscribe(l -> log.info("{}", l));
        // May require the use of Thread.sleep(...) in order to prevent the logging in the console from blending with the test.
        // As always, it is not a guaranteed thing, you're dealing with threads here.
        Thread.sleep(2000);

        log.info("---------------------------------------------------------------------");

        StepVerifier.create(mono)
                .expectSubscription()
                .thenConsumeWhile(l -> {
                    Assertions.assertFalse(l.isEmpty());
//                    Assertions.assertTrue(l.isEmpty());
                    return true;
                })
                .verifyComplete();
    }
}
