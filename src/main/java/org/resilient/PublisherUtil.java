package org.resilient;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class PublisherUtil {
    private Map<String, PublishRequest> publishRequestMap = new HashMap<>();
    private final TopicAdminClient topicAdminClient;

    public PublisherUtil(String endpoint) throws IOException {
        BatchingSettings batchingSettings = BatchingSettings.newBuilder()
                .setDelayThreshold(Duration.ofSeconds(6))
                .setElementCountThreshold(1L)
                .setRequestByteThreshold(1000000L)
                .build();

        TopicAdminSettings.Builder topicAdminSettingsBuilder = TopicAdminSettings.newBuilder();
        topicAdminSettingsBuilder.getStubSettingsBuilder().setEndpoint(endpoint);
        topicAdminSettingsBuilder.publishSettings().setBatchingSettings(batchingSettings);

        TopicAdminSettings topicAdminSettings = topicAdminSettingsBuilder.build();

        topicAdminClient = TopicAdminClient.create(topicAdminSettings);
    }

    public PublishRequest getPublishRequest(String projectId, String topicId) {
        TopicName topicName = TopicName.of(projectId, topicId);
        String resourceName = getResourceName(topicName);
        if (publishRequestMap.containsKey(resourceName)) return publishRequestMap.get(resourceName);

        PublishRequest publishRequest = PublishRequest.newBuilder().setTopic(getResourceName(topicName)).build();
        publishRequestMap.put(resourceName, publishRequest);

        return publishRequest;
    }

    public void publish(String message, TopicName topicName) {
        PublishRequest publishRequest = getPublishRequest(topicName.getProject(), topicName.getTopic());
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        PublishRequest.newBuilder(publishRequest).addMessages(pubsubMessage).build();

        ApiFuture<PublishResponse> apiFuture = topicAdminClient.publishCallable().futureCall(publishRequest);

    }

    private String getResourceName(TopicName topicName) {
        return "projects/" + topicName.getProject() + "/topics/" + topicName.getTopic();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String endpoint = "asia-south1-pubsub.googleapis.com:443";
        PublisherUtil publisherUtil = new PublisherUtil(endpoint);

        List<String> messages = new ArrayList<>(Arrays.asList(
                "Hello",
                "there",
                "abhi!"
        ));
        String projectId = "solar-program-329718";
        String topicId = "newone";
        TopicName topicName = TopicName.of(projectId, topicId);

        for (String message : messages) {
            publisherUtil.publish(message, topicName);
            break;
        }
        System.out.println("Published messages asynchronously");

        TimeUnit.SECONDS.sleep(12);
    }
}
