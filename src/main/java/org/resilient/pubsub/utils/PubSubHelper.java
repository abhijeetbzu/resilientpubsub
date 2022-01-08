package org.resilient.pubsub.utils;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

public class PubSubHelper {
    public static PublishRequest getPublishRequest(TopicName topicName, String message) {
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data)//.setPublishTime().setOrderingKey()
                .build();
        return PublishRequest.newBuilder().setTopic(getResourceName(topicName))
                .addMessages(pubsubMessage).build();
    }

    public static String getResourceName(TopicName topicName) {
        return "projects/" + topicName.getProject() + "/topics/" + topicName.getTopic();
    }
}
