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
import java.time.Duration;
import java.util.List;

/*
 * Note to self: you used the word 'flux' (uncased) meaning 'stream of data/events' in the comments.
 * */
@Slf4j
public class OperatorsTest {

    @Test
    void Operator_SubscribeOn() {
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
    void Operator_PublishOn() {
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
    void MultipleOperators_SubscribeOn() {
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
    void MultipleOperators_PublishOn() {
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
    void MergingOperators_PublishOnWithSubscribeOn() {
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
    void MergingOperators_SubscribeOnWithPublishOn() {
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
    void Operator_FromCallable() throws InterruptedException {
        /*
         * This is the way in which you should communicate with external APIs.
         * */
        final Mono<List<String>> mono = Mono
                .fromCallable(() -> Files.readAllLines(Path.of("my-text.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic()); // boundedElastic() is recommended by documentation for I/O operations.

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

    @Test
    void Operator_SwitchIfEmpty() {
        final String text = "Heide";
        final Flux<Object> flux = generateEmptyFlux()
                .switchIfEmpty(Flux.just(text, text, text)) // Imperative way of using switch/if
                .take(2)
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(text, text)
                .verifyComplete();
    }

    private Flux<Object> generateEmptyFlux() {
        return Flux.empty();
    }

    @Test
    void Operator_Defer() throws InterruptedException {
        final Mono<Long> mono = Mono
                // Generates a different stream of events for each Subscriber.
                .defer(() -> Mono.just(System.currentTimeMillis()));

        mono.subscribe(l -> log.info("{}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("{}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("{}", l));
        Thread.sleep(100);
    }

    @Test
    void Operator_ConcatAndConcatWith() {
        final Flux<String> flux1 = Flux.just("A", "B");
        final Flux<String> flux2 = Flux.just("C", "D");

//        final Flux<String> concat = Flux.concat(flux1, flux2).log();
        final Flux<String> concat = flux1.concatWith(flux2).log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("A", "B", "C", "D")
                .verifyComplete();
    }

    @Test
    void Operator_CombineLast() {
//        final Flux<String> flux1 = Flux.just("A", "B").delayElements(Duration.ofMillis(1000));
        final Flux<String> flux1 = Flux.just("A", "B");
        final Flux<String> flux2 = Flux.just("C", "D");

        /*
         * You MUST NOT create your logic trying to predict the result of the combination,
         * because the result is bounded to the behavior of the flux (event stream).
         * The first flux MAY OR MAY NOT be processed before certain elements in the second flux.
         * The same goes for the second flux.
         * */
        final Flux<String> combinedLatest = Flux
                .combineLatest(flux1, flux2, (s1, s2) -> s1 + ":" + s2)
                .log();

        StepVerifier
                .create(combinedLatest)
                .expectSubscription()
                .expectNext("B:C", "B:D") // Lucky based, not reliable.
                .verifyComplete();
    }
}
