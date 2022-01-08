package org.resilient.pubsub.factory;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;

public interface IngestionClientFactory {
    TopicAdminClient getTopicAdminClient();

    Publisher getPublisher();
}
