package org.resilient.pubsub.factory;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import java.time.Duration;
import java.util.Optional;

public class CircuitBreakerFactory {
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    public CircuitBreakerFactory() {
        CircuitBreakerConfig config = CircuitBreakerConfig
                .custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .permittedNumberOfCallsInHalfOpenState(20)
                .waitDurationInOpenState(Duration.ofMillis(30000))
                .failureRateThreshold(70.0f)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .minimumNumberOfCalls(10)
                .maxWaitDurationInHalfOpenState(Duration.ofSeconds(20))
                .build();

        circuitBreakerRegistry = CircuitBreakerRegistry.of(config);
    }

    public CircuitBreakerFactory(CircuitBreakerConfig circuitBreakerConfig) {
        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
    }

    public CircuitBreakerFactory(CircuitBreakerRegistry circuitBreakerRegistry) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    public CircuitBreaker getOrCreate(String name) {
        Optional<CircuitBreaker> optionalCircuitBreaker = circuitBreakerRegistry.find(name);
        return optionalCircuitBreaker.orElse(circuitBreakerRegistry.circuitBreaker(name));
    }
}
