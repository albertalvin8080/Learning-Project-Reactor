package com.albert.firsttest;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
public class AppTest {

    @Test
    public void Operators_Test() {
        final String text = "Gena";
        final Mono<String> mono = Mono.just(text)
                .log();

        mono.subscribe();
        log.info("---------------------------------------------------------");
        StepVerifier.create(mono)
                .expectNext(text)
                .verifyComplete();
    }
}
