package org.resilient.pubsub.ingestion;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.TopicName;

import java.util.concurrent.Future;

public interface IPublisher<T> {
    Future<T> publish(PublishRequest publishRequest, TopicName topicName) throws Throwable;
}
