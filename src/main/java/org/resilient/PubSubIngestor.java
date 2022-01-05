package org.resilient;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

public class PubSubIngestor implements IIngestor {
    private final IngestionClientFactory ingestionClientFactory;

    public PubSubIngestor(IngestionClientFactory ingestionClientFactory) {
        this.ingestionClientFactory = ingestionClientFactory;
    }

    public static String getResourceName(TopicName topicName) {
        return "projects/" + topicName.getProject() + "/topics/" + topicName.getTopic();
    }

    @Override
    public UnaryCallable<PublishRequest, PublishResponse> getPublishCallable() {
        TopicAdminClient topicAdminClient = ingestionClientFactory.getTopicAdminClient();
        return topicAdminClient.publishCallable();
    }

    @Override
    public PublishRequest getPublishRequest(TopicName topicName, String message) {
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data)//.setPublishTime().setOrderingKey()
                .build();
        PublishRequest publishRequest = PublishRequest.newBuilder().setTopic(getResourceName(topicName))
                .addMessages(pubsubMessage).build();
        return publishRequest;
    }
}
