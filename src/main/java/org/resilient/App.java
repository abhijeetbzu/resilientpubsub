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

/**
 * Hello world!
 */
public class App {
    public static TopicAdminClient getTopicAdminClient(String endpoint) throws IOException {
        BatchingSettings batchingSettings = BatchingSettings.newBuilder()
                .setDelayThreshold(Duration.ofSeconds(60))
                .setElementCountThreshold(5L)
                .setRequestByteThreshold(1000000L)
                .build();

        TopicAdminSettings.Builder topicAdminSettingsBuilder = TopicAdminSettings.newBuilder();
        topicAdminSettingsBuilder.getStubSettingsBuilder().setEndpoint(endpoint);
        topicAdminSettingsBuilder.publishSettings().setBatchingSettings(batchingSettings);

        TopicAdminSettings topicAdminSettings = topicAdminSettingsBuilder.build();

        return TopicAdminClient.create(topicAdminSettings);
    }

    public static void createTopic(String projectId, String topicId, TopicAdminClient topicAdminClient) {
        TopicName topicName = TopicName.of(projectId, topicId);
        topicAdminClient.createTopic(topicName);
    }

    public static String getResourceName(TopicName topicName) {
        return "projects/" + topicName.getProject() + "/topics/" + topicName.getTopic();
    }

    public static ApiFuture<PublishResponse> publishMessageAsync(TopicAdminClient topicAdminClient, TopicName topicName
            , String message) {
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        PublishRequest publishRequest = PublishRequest.newBuilder().setTopic(getResourceName(topicName))
                .addMessages(pubsubMessage).build();

        return topicAdminClient.publishCallable().futureCall(publishRequest);
    }

    public static ApiFuture<PublishResponse> publishMessagesAsync(TopicAdminClient topicAdminClient,
                                                                  TopicName topicName, List<String> messages) {
        List<PubsubMessage> pubsubMessages = new ArrayList<>();
        for (String message : messages) {
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
            pubsubMessages.add(pubsubMessage);
        }
        PublishRequest publishRequest = PublishRequest.newBuilder().setTopic(getResourceName(topicName))
                .addAllMessages(pubsubMessages).build();
        return topicAdminClient.publishCallable().futureCall(publishRequest);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String endpoint = "asia-south1-pubsub.googleapis.com:443";

        TopicAdminClient topicAdminClient = getTopicAdminClient(endpoint);
        System.out.println(topicAdminClient.getSettings().getEndpoint());


        String projectId = "fk-sanbox-fdp-temp-1";
        List<String> topics = new ArrayList<>(Arrays.asList(
                "newone",
                "newone1",
                "newone2",
                "newone3",
                "newone4",
                "newone5"
        ));
//        for (String topic : topics)
//            createTopic(projectId, topic, topicAdminClient);
        Map<String,TopicPublishTask> threadMap = new HashMap<>();
        for (String topic : topics){
            TopicPublishTask topicPublishTask = new TopicPublishTask(topicAdminClient,projectId,topic);
            threadMap.put(topic,topicPublishTask);
            topicPublishTask.run();
        }

        List<String> messages = new ArrayList<>(Arrays.asList(
                "Hello",
                "there",
                "world!",
                "abhi!"
        ));
        for (String message : messages) {
            System.out.println("Publishing message '" + message + "'----------------");
            for (String topic : topics) {
                System.out.println("Publishing to topic " + topic);
                TopicName topicName = TopicName.of(projectId, topic);
                ApiFuture<PublishResponse> apiFuture = publishMessageAsync(topicAdminClient, topicName, message);

            }
            break;
        }

//        ApiFuture<PublishResponse> apiFuture = publishMessagesAsync(topicAdminClient, topicName, messages);
//        System.out.println("Published message asynchronously-------");
//

//
//        System.out.println("Added callback-------");

        //apiFuture.get();
//        while (!apiFuture.isDone()) continue;

        TimeUnit.SECONDS.sleep(120);
        System.out.println("Hello World!");
    }
}
