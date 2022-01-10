package org.resilient.pubsub.ingestion;

import com.google.api.core.ApiFutureCallback;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.TopicName;
import io.vavr.control.Try;
import org.resilient.pubsub.example.Demo;
import org.resilient.pubsub.utils.PubSubRequestFutureHolder;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class PubSubCallback implements ApiFutureCallback<PublishResponse> {
    private final PublishRequest publishRequest;
    private final ResilientPublisher resilientPublisher;
    private final TopicName topicName;
    private final String message;

    public PubSubCallback(PublishRequest publishRequest, ResilientPublisher resilientPublisher
            , TopicName topicName) {
        this.publishRequest = publishRequest;
        this.resilientPublisher = resilientPublisher;
        this.message = publishRequest.getMessages(0).getData().toStringUtf8();
        this.topicName = topicName;
    }

    @Override
    public void onFailure(Throwable throwable) {
        //updating circuit breaker that request failed
        resilientPublisher.getCircuitBreaker().executeTrySupplier(() -> {
            return Try.of(() -> {
                throw new Exception();
            });
        });

        Demo.requestExecutionInfoHolder.append(publishRequest, "Error publishing message : " + message + " using " +
                resilientPublisher.getCircuitBreaker().getName());

        /*  calling fallback publisher to handle failed request
            and updating futureObject of request with fallback publisher returned future object
            so that future object returned to client from publisher publish method
            can do get() on updated future
         */
        if (resilientPublisher.getFallbackPublisher() != null) {
            AtomicReference<Future<PublishResponse>> apiFuture = PubSubRequestFutureHolder.futureMap.get(publishRequest);
            apiFuture.set(resilientPublisher.getFallbackPublisher().publish(publishRequest, topicName));
        }
    }

    @Override
    public void onSuccess(PublishResponse publishResponse) {
        // Once published, returns server-assigned message ids (unique within the topic)
        Demo.requestExecutionInfoHolder.append(publishRequest, "Published message ID: " + message + " using " +
                resilientPublisher.getCircuitBreaker().getName());
    }
}