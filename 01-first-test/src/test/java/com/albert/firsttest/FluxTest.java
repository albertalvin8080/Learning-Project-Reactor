package com.albert.firsttest;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
public class FluxTest {
    private final List<String> names = List.of("Heide", "Old Golem", "Heide Knight");

    @Test
    void Flux_FromIterable() {
        final Flux<String> flux = Flux.fromIterable(names).log();

        StepVerifier.create(flux)
                .expectNext(names.toArray(new String[0]))
                .verifyComplete();
    }

    @Test
    void Flux_FromRange() {
        final Flux<Integer> flux = Flux.range(1, 10)
                .log()
                // handler() is preferred to map() when the code may throw an exception
                .handle((i, sink) -> {
                    if(i > 3) {
                        sink.error(new IndexOutOfBoundsException("Index above 3"));
                        return;
                    }
                    sink.next(i);
                });

        flux.subscribe(
                i -> log.info("i: {}", i),
                Throwable::printStackTrace,
                () -> log.info("COMPLETED"),
                s -> s.request(4)
        );

        log.info("-------------------------------------------------------{}", System.lineSeparator());

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .verifyError(IndexOutOfBoundsException.class);
    }

    @Test
    void Flux_BackpressureWithCustomSubscriber() {
        final Flux<Integer> flux = Flux.range(1, 10).log();

        flux.subscribe(new Subscriber<Integer>() {
            private final int requestCount = 3;
            private int requestIndex = 0;
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                subscription.request(requestCount);
            }

            @Override
            public void onNext(Integer integer) {
                log.info("i: {}", integer);
                if(++requestIndex == requestCount)
                {
                    requestIndex = 0;
                    subscription.request(requestCount);
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {
                log.info("CUSTOM SUBSCRIBER COMPLETED");
            }
        });
    }

    @Test
    void Flux_BackpressureWithBaseSubscriber() {
        final Flux<Integer> flux = Flux.range(1, 10).log();

        flux.subscribe(new BaseSubscriber<Integer>() {
            private final int requestCount = 2;
            private int requestIndex = 0;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                if(++requestIndex >= requestCount) {
                    requestIndex = 0;
                    request(requestCount);
                }
            }

            @Override
            protected void hookOnComplete() {
                log.info("COMPLETED");
            }
        });
    }

    @Test
    void Flux_Interval() throws InterruptedException {
        final Flux<Long> flux = Flux.interval(Duration.ofMillis(100));

        flux.subscribe(l -> log.info("{}", l));

        // This is necessary because the interval() executes in a separate thread from the main.
        // When the main thread ends, it doesn't stop unless it's explicitly stated to do so.
        Thread.sleep(3000);
    }

    @Test
    void Flux_Interval_VirtualTime() {
        // The Flux<> MUST be created inside withVirtualTime(), otherwise the
        // method won't know if the Scheduler was created appropriately
        StepVerifier.withVirtualTime(this::createFluxInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(24)) // makes sure there are no events before the stipulated time.
                .thenAwait(Duration.ofDays(2))
                .expectNext(0L)
                .expectNext(1L)
                .thenCancel() // if not present, the interval() would produce events indefinitely.
                .verify();

        // another way of using thenAwait()
        StepVerifier.withVirtualTime(this::createFluxInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(24))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    private Flux<Long> createFluxInterval() {
        return Flux.interval(Duration.ofDays(1)).log();
    }
}
