package org.resilient.pubsub.ingestion;

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
import org.resilient.pubsub.example.Demo;
import org.resilient.pubsub.factory.CircuitBreakerFactory;
import org.resilient.pubsub.factory.IngestionClientFactory;
import org.resilient.pubsub.utils.PubSubHelper;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class ResilientPublisher implements IPublisher<PublishResponse>, IResilient {
    private final ResilientPublisher fallbackPublisher;
    private final IngestionClientFactory ingestionClientFactory;
    private final CircuitBreakerFactory circuitBreakerFactory;
    private final String circuitBreakerName;

    public ResilientPublisher(IngestionClientFactory ingestionClientFactory,
                              CircuitBreakerFactory circuitBreakerFactory,
                              String circuitBreakerName) {
        this(null, ingestionClientFactory, circuitBreakerFactory, circuitBreakerName);
    }

    public ResilientPublisher(ResilientPublisher fallbackPublisher, IngestionClientFactory ingestionClientFactory
            , CircuitBreakerFactory circuitBreakerFactory, String circuitBreakerName) {
        this.fallbackPublisher = fallbackPublisher;
        this.circuitBreakerFactory = circuitBreakerFactory;
        this.ingestionClientFactory = ingestionClientFactory;
        this.circuitBreakerName = circuitBreakerName;
    }

    public Future<PublishResponse> publish2(TopicName topicName, String message) {
        PublishRequest publishRequest = PubSubHelper.getPublishRequest(topicName, message);
        PubSubCallback pubSubCallback = new PubSubCallback(publishRequest, this, topicName);
        String cbname = getCircuitBreaker().getName();

        UnaryCallable<PublishRequest, PublishResponse> publishCallable = getPublishCallable();
        CircuitBreaker circuitBreaker = getCircuitBreaker();
        ApiFuture<PublishResponse> apiFuture = publishCallable.futureCall(publishRequest);
        ApiFutures.addCallback(
                apiFuture,
                pubSubCallback,
                MoreExecutors.directExecutor());

        CheckedFunction0<PublishResponse> decorated = Decorators
                .ofCheckedSupplier(() -> {
                    return apiFuture.get();
                })
                .withCircuitBreaker(circuitBreaker)
                .withFallback(
                        Collections.singletonList(CallNotPermittedException.class),
                        e -> {
                            System.out.println("Call not permitted for message: " + message + " using " +
                                    cbname);
                            ResilientPublisher fallbackPublisher = getFallbackPublisher();
                            if (fallbackPublisher != null)
                                return fallbackPublisher.publish(publishRequest, topicName).get();
                            return null;
                        })
                .decorate();


        CompletableFuture<PublishResponse> c = Decorators.ofCompletionStage(() -> {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return decorated.apply();
                } catch (Throwable e) {
                    return null;
                }
            });
        }).get().toCompletableFuture();


        return c;
    }

    @Override
    public Future<PublishResponse> publish(final PublishRequest publishRequest, TopicName topicName) {
        PubSubCallback pubSubCallback = new PubSubCallback(publishRequest, this, topicName);

        UnaryCallable<PublishRequest, PublishResponse> publishCallable = getPublishCallable();
        ApiFuture<PublishResponse> apiFuture = publishCallable.futureCall(publishRequest);

        if (!Demo.futureMap.containsKey(publishRequest)) {
            //fresh publish request, add future object to global future map
            Demo.futureMap.put(publishRequest, new AtomicReference<>(apiFuture));
        }

        /*  else request already there in global future map, means this request has come from other publisher
            and this publisher is fallback for that publisher and callback for future object returned from
            that publisher will add future object returned from this publisher into global future map
        */
        ApiFutures.addCallback(
                apiFuture,
                pubSubCallback,
                MoreExecutors.directExecutor());
        return new IngestionFuture(apiFuture, publishRequest);
    }

    @Override
    public CircuitBreaker getCircuitBreaker() {
        return circuitBreakerFactory.getOrCreate(circuitBreakerName);
    }

    @Override
    public ResilientPublisher getFallbackPublisher() {
        return fallbackPublisher;
    }

    private UnaryCallable<PublishRequest, PublishResponse> getPublishCallable() {
        return ingestionClientFactory.getTopicAdminClient().publishCallable();
    }
}