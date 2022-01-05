package org.resilient;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;

public interface IngestionClientFactory {
    TopicAdminClient getTopicAdminClient();

    Publisher getPublisher();
}
