package org.resilient;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.decorators.Decorators;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.Supplier;

@Slf4j
public class CircuitUtil {
    public String hello() throws Exception {
        throw new Exception("h");
    }

    public String failed(){
        return "Failed!";
    }

    public static void main(String[] args) throws Exception{
        CircuitBreakerConfig config = CircuitBreakerConfig
                .custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .failureRateThreshold(25.0f)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(4)
                .build();

        CircuitUtil circuitUtil = new CircuitUtil();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        CircuitBreaker circuitBreaker = registry.circuitBreaker("flightSearchService");

        Supplier<String> flightsSupplier = () -> {
            try {
                return circuitUtil.hello();
            } catch (Exception e) {
                return "Exception";
            }
        };
        Supplier<String> decorated = Decorators
                .ofSupplier(flightsSupplier)
                .withCircuitBreaker(circuitBreaker)
                .withFallback(Arrays.asList(CallNotPermittedException.class),
                        e -> circuitUtil.failed())
                .decorate();

        System.out.println(decorated.get());
    }
}
