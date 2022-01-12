package org.resilient.pubsub.factory;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.resilient.pubsub.ingestion.ResilientPublisher;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class ResilientPublisherFactory {
    public final CircuitBreakerFactory circuitBreakerFactory;
    private AtomicReference<ResilientPublisher> primaryPublisher;

    public ResilientPublisherFactory(CircuitBreakerFactory circuitBreakerFactory) {
        this.circuitBreakerFactory = circuitBreakerFactory;
    }

    public ResilientPublisher getResilientPublisher(ResilientPublisher fallbackPublisher,
                                                    String endpoint, String circuitBreakerName)
            throws IOException {
        PublisherComponentFactory publisherComponentFactory = new PublisherComponentFactory(endpoint);
        TopicAdminClientFactory topicAdminClientFactory = new TopicAdminClientFactory(publisherComponentFactory);
        return new ResilientPublisher(fallbackPublisher,
                topicAdminClientFactory, circuitBreakerFactory, circuitBreakerName);
    }

    public ResilientPublisher getResilientPublisher(String endpoint, String circuitBreakerName)
            throws IOException {
        return getResilientPublisher(null, endpoint, circuitBreakerName);
    }


    public ResilientPublisher getActivePublisher() throws Exception {
        if (primaryPublisher == null) throw new Exception("No primary publisher set");
        ResilientPublisher active = primaryPublisher.get();
        while (active.getCircuitBreaker().getState() != CircuitBreaker.State.CLOSED &&
                active.getCircuitBreaker().getState() != CircuitBreaker.State.HALF_OPEN) {
            active = active.getFallbackPublisher();
        }
        if (active == null) {
            throw new Exception("No active publisher");
        }
        return active;
    }

    public void setPrimaryPublisher(ResilientPublisher resilientPublisher) {
        if (primaryPublisher == null) primaryPublisher = new AtomicReference<>(resilientPublisher);
        primaryPublisher.set(resilientPublisher);
    }
}
