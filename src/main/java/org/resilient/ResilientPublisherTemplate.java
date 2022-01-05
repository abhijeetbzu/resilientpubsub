package org.resilient;

import com.google.pubsub.v1.PublishResponse;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

public abstract class ResilientPublisherTemplate implements IPublisher<PublishResponse> {

    abstract CircuitBreaker getCircuitBreaker();

    abstract ResilientPublisher getFallbackPublisher();


}
