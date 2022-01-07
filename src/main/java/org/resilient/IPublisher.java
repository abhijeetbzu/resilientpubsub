package org.resilient;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.TopicName;

import java.util.concurrent.Future;

public interface IPublisher<T> extends IIngestor {
    Future<T> publish(PublishRequest publishRequest, TopicName topicName) throws Throwable;
}
