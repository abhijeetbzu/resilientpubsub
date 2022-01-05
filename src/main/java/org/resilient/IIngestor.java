package org.resilient;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.TopicName;

public interface IIngestor {
    UnaryCallable<PublishRequest, PublishResponse> getPublishCallable();
    PublishRequest getPublishRequest(TopicName topicName, String message);
}
