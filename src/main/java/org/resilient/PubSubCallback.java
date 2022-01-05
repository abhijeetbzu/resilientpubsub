package org.resilient;

import com.google.api.core.ApiFutureCallback;
import com.google.api.gax.rpc.ApiException;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;

import java.util.List;

public class PubSubCallback implements ApiFutureCallback<PublishResponse> {
    private final PublishRequest publishRequest;
    private final ResilientPublisher resilientPublisher;
    private final String message;

    public PubSubCallback(PublishRequest publishRequest, ResilientPublisher resilientPublisher, String message) {
        this.publishRequest = publishRequest;
        this.resilientPublisher = resilientPublisher;
        this.message = message;
    }

    @Override
    public void onFailure(Throwable throwable) {
        if (throwable instanceof ApiException) {
            ApiException apiException = ((ApiException) throwable);
            // details on the API exception
            //System.out.println(apiException.getStatusCode().getCode());
            //System.out.println(apiException.isRetryable());
            //System.out.println(Arrays.toString(apiException.getStackTrace()));
        }
        System.out.println("Error publishing message : " + message + " using " +
                resilientPublisher.getCircuitBreaker().getName());
    }

    @Override
    public void onSuccess(PublishResponse publishResponse) {
        // Once published, returns server-assigned message ids (unique within the topic)
        List<String> messageIds = publishResponse.getMessageIdsList();
        System.out.println("Published message ID: " + message + " using " +
                resilientPublisher.getCircuitBreaker().getName());
    }
}