package com.albert.firsttest;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

@Slf4j
public class AppTest
{
    private final String text = "Gena";

    @Test
    void Mono_Testing() {
        final Mono<String> mono = Mono.just(text)
                .log();

        mono.subscribe();
        log.info("---------------------------------------------------------");

        StepVerifier.create(mono)
                .expectNext(text)
                .verifyComplete();
    }

    @Test
    void Mono_SubscriberConsumer() {
        final Mono<String> mono = Mono.just(text)
                .log();

        mono.subscribe(s -> log.info("Value: {}", s));
        log.info("---------------------------------------------------------");

        StepVerifier.create(mono)
                .expectNext(text)
                .verifyComplete();
    }

    @Test
    void Mono_ErrorConsumer() {
        final Mono<String> mono = Mono.just(text)
                .log()
                .map(s -> {
                    throw new RuntimeException("Programmed Error");
                }); // for testing only

        mono.subscribe(
                s -> log.info("Value: {}", s),
//                e -> log.error("Something bad happened: {}", e.getMessage()) // error consumer
                Throwable::printStackTrace
        );
        log.info("---------------------------------------------------------");

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void Mono_CompleteConsumer() {
        final Mono<String> mono = Mono.just(text)
                .log();

        mono.subscribe(
                s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("COMPLETED")
        );
        log.info("---------------------------------------------------------");

        StepVerifier.create(mono)
                .expectNext(text)
                .verifyComplete();
    }

    @Test
    void Mono_SubscriptionConsumer() {
        final Mono<String> mono = Mono.just(text).log();

        mono.subscribe(
                s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("COMPLETED"),
                Subscription::cancel
        );
        log.info("---------------------------------------------------------");

        // The verifyComplete() method checks if the Mono has completed its emission of data.
        // However, since the subscription was cancelled prematurely, the Mono is unable to complete
        // as expected. This discrepancy causes the program to hang or “get stuck” because
        // verifyComplete() is waiting for a completion signal that will never come.
        StepVerifier.create(mono)
                .consumeSubscriptionWith(Subscription::cancel)
                .verifyComplete();
    }
}
