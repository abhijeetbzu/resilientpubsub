package org.resilient;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.TopicName;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.decorators.Decorators;
import io.vavr.CheckedFunction0;
import lombok.SneakyThrows;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class ResilientPublisher extends ResilientPublisherTemplate {
    private final ResilientPublisher fallbackPublisher;
    private final PubSubIngestor pubSubIngestor;
    private final CircuitBreakerFactory circuitBreakerFactory;
    private final String circuitBreakerName;

    public ResilientPublisher(PubSubIngestor pubSubIngestor, CircuitBreakerFactory circuitBreakerFactory,
                              String circuitBreakerName) {
        this(null, pubSubIngestor, circuitBreakerFactory, circuitBreakerName);
    }

    public ResilientPublisher(ResilientPublisher resilientPublisher, PubSubIngestor pubSubIngestor
            , CircuitBreakerFactory circuitBreakerFactory, String circuitBreakerName) {
        fallbackPublisher = resilientPublisher;
        this.circuitBreakerFactory = circuitBreakerFactory;
        this.pubSubIngestor = pubSubIngestor;
        this.circuitBreakerName = circuitBreakerName;
    }

    @Override
    public Future<PublishResponse> publish(TopicName topicName, String message) {

        PublishRequest publishRequest = getPublishRequest(topicName, message);
        PubSubCallback pubSubCallback = new PubSubCallback(publishRequest, this, message);
        String cbname = getCircuitBreaker().getName();

        FutureTask<PublishResponse> futureTask = new FutureTask<>(new Callable<PublishResponse>() {
            @SneakyThrows
            @Override
            public PublishResponse call() throws Exception {
                UnaryCallable<PublishRequest, PublishResponse> publishCallable = getPublishCallable();
                CircuitBreaker circuitBreaker = getCircuitBreaker();

                CheckedFunction0<PublishResponse> decorated = Decorators
                        .ofCheckedSupplier(() -> {
                            ApiFuture<PublishResponse> apiFuture = publishCallable.futureCall(publishRequest);
                            ApiFutures.addCallback(
                                    apiFuture,
                                    pubSubCallback,
                                    MoreExecutors.directExecutor());
                            return apiFuture.get();
                        })
                        .withCircuitBreaker(circuitBreaker)
                        .withFallback(
                                Collections.singletonList(CallNotPermittedException.class),
                                e -> {
                                    System.out.println("Call not permitted for message: " + message + " using " +
                                            cbname);
                                    ResilientPublisher fallbackPublisher = getFallbackPublisher();
                                    if(fallbackPublisher != null)
                                        return fallbackPublisher.publish(topicName, message).get();
                                    return null;
                                })
                        .decorate();

                return decorated.apply();
            }
        });

        new Thread(new Runnable() {
            @Override
            public void run() {
                futureTask.run();
            }
        }).start();
        return futureTask;
    }

    @Override
    CircuitBreaker getCircuitBreaker() {
        return circuitBreakerFactory.getOrCreate(circuitBreakerName);
    }

    @Override
    ResilientPublisher getFallbackPublisher() {
        return fallbackPublisher;
    }

    @Override
    public UnaryCallable<PublishRequest, PublishResponse> getPublishCallable() {
        return pubSubIngestor.getPublishCallable();
    }

    @Override
    public PublishRequest getPublishRequest(TopicName topicName, String message) {
        return pubSubIngestor.getPublishRequest(topicName, message);
    }
}
