package org.resilient.pubsub.ingestion;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

public interface IResilient {
    CircuitBreaker getCircuitBreaker();
    ResilientPublisher getFallbackPublisher();
}
