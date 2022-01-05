package org.resilient;

import com.google.pubsub.v1.PublishResponse;

import java.util.concurrent.FutureTask;

public class PublisherFutureResponse {
    public final FutureTask<PublisherFutureResponse> futureResponse;
    public final PublishResponse publishResponse;

    public PublisherFutureResponse(FutureTask<PublisherFutureResponse> futureResponse) {
        this(futureResponse, null);
    }

    public PublisherFutureResponse(PublishResponse publishResponse) {
        this(null, publishResponse);
    }

    public PublisherFutureResponse(FutureTask<PublisherFutureResponse> futureResponse, PublishResponse publishResponse) {
        this.futureResponse = futureResponse;
        this.publishResponse = publishResponse;
    }
}
