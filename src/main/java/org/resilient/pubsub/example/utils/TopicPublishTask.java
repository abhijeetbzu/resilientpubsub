package org.resilient.pubsub.example.utils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class TopicPublishTask implements Runnable {
    public BlockingDeque<String> messages = new LinkedBlockingDeque<>();
    private TopicName topicName;
    private TopicAdminClient topicAdminClient;

    public TopicPublishTask(TopicAdminClient topicAdminClient, String projectId, String topicId) {
        topicName = TopicName.of(projectId, topicId);
        this.topicAdminClient = topicAdminClient;
    }

    public static ApiFuture<PublishResponse> publishMessageAsync(TopicAdminClient topicAdminClient, TopicName topicName
            , String message) {
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        PublishRequest publishRequest = PublishRequest.newBuilder().setTopic(getResourceName(topicName))
                .addMessages(pubsubMessage).build();

        return topicAdminClient.publishCallable().futureCall(publishRequest);
    }

    public static String getResourceName(TopicName topicName) {
        return "projects/" + topicName.getProject() + "/topics/" + topicName.getTopic();
    }

    @Override
    public void run() {
        while (true) {
            try {
                String message = messages.take();
                ApiFuture<PublishResponse> apiFuture = publishMessageAsync(topicAdminClient, topicName, message);
                ApiFutures.addCallback(
                        apiFuture,
                        new ApiFutureCallback<PublishResponse>() {
                            @Override
                            public void onFailure(Throwable throwable) {
                                System.out.println("error");
                            }

                            @Override
                            public void onSuccess(PublishResponse publishResponse) {
                                System.out.println("success: " + publishResponse.getMessageIds(0));
                            }
                        },
                        MoreExecutors.directExecutor());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
