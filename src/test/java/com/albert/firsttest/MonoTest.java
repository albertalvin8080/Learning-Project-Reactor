package com.albert.firsttest;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/*
 * Note to self: remember to see the theoretical concepts in your notebook.
 * */
@Slf4j
public class MonoTest {
    private final String text = "Gena";

    @Test
    void Mono_Testing() {
        final Mono<String> mono = Mono.just(text)
                .log();

        mono.subscribe();
        log.info("---------------------------------------------------------");

        StepVerifier.create(mono)
                .expectSubscription()
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
                .handle((s, sink) -> {
                    sink.error(new RuntimeException("Programmed Error"));
                }); // for testing only

        mono.subscribe(
                s -> log.info("Value: {}", s),
                e -> log.error("Something bad happened: {}", e.getMessage()) // error consumer
//                Throwable::printStackTrace
        );
        log.info("---------------------------------------------------------");

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void Mono_CompleteConsumer() {
        final Mono<String> mono = Mono.just(text)
                .map(String::toUpperCase)
                .log();

        mono.subscribe(
                s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("COMPLETED") // complete consumer
        );
        log.info("---------------------------------------------------------");

        StepVerifier.create(mono)
                .expectNext(text.toUpperCase())
                .verifyComplete();
    }

    @Test
    void Mono_SubscriptionConsumer() {
        final Mono<String> mono = Mono.just(text).log();

        // Note: we're trying to test the Subscriber here, not the Publisher.
        mono.subscribe(
                str -> log.info("Value: {}", str),
                Throwable::printStackTrace,
                () -> log.info("COMPLETED"),
//                Subscription::cancel            // subscription consumer
                s -> s.request(5)
        );
        log.info("---------------------------------------------------------");

        // The verifyComplete() method checks if the Mono has completed its emission of data.
        // However, since the subscription was cancelled prematurely, the Mono is unable to complete
        // as expected. This discrepancy causes the program to hang or “get stuck” because
        // verifyComplete() is waiting for a completion signal that will never come.
//        StepVerifier.create(mono)
//                .consumeSubscriptionWith(Subscription::cancel)
//                .verifyComplete();
    }

    @Test
    void Mono_DoOnSubscribe_Request_Next_Success() {
        final Mono<String> mono = Mono.just(text)
                .doOnSubscribe(s -> log.info("SUBSCRIBED: {}", s))
                .doOnRequest(l -> log.info("REQUESTED: {}", l))
                .map(String::toLowerCase)
                .doOnNext(s -> log.info("NEXT: {}", s))
                .doOnSuccess(s -> log.info("SUCCESS: {}", s));

        StepVerifier.create(mono)
                .expectNext(text.toLowerCase())
                .verifyComplete();
    }

    @Test
    void Mono_DoOnError() {
        final Mono<Object> mono = Mono.error(new IllegalArgumentException("It's what it says."))
                .doOnError(e -> log.info("ERROR: {}", e.getMessage()));

        StepVerifier.create(mono)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void Mono_OnErrorReturn_OnErrorResume() {
        final String fallbackValue = "Fallback Value";

        final Mono<Object> mono = Mono.error(new IllegalArgumentException("It's what it says."))
                .doOnError(e -> log.info("ERROR 1: {}", e.getMessage()))
                // first fallback
                .onErrorReturn(fallbackValue)
                .flatMap(s -> Mono.error(new RuntimeException("Check the documentation.")))
                // second fallback
                .onErrorResume(RuntimeException.class, e -> {
                    log.info("ERROR 2: {}", e.getMessage());
                    return Mono.just(fallbackValue);
                })
                .log();

        StepVerifier.create(mono)
                .expectNext(fallbackValue)
                .verifyComplete();
    }
}
