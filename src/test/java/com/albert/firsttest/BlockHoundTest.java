package com.albert.firsttest;

import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class BlockHoundTest
{
    // Example extracted from https://github.com/reactor/BlockHound
    @Test
    public void Test() {
//        BlockHound.install();
        BlockHound.install(builder -> builder.allowBlockingCallsInside( // Allows blocking int this case
                "reactor.core.publisher.MonoDelay$MonoDelayRunnable",
                "run")
        );

        Mono.delay(Duration.ofSeconds(1))
                .doOnNext(it -> {
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .block();
    }
}
