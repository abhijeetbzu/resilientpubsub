package org.resilient;

import com.google.cloud.pubsub.v1.TopicAdminClient;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BatchTesting {
    public static void main(String[] args) throws IOException {

        String endpoint = "asia-south1-pubsub.googleapis.com:443";

        TopicAdminClient topicAdminClient = App.getTopicAdminClient(endpoint);
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


        Map<String, TopicPublishTask> threadMap = new HashMap<>();

        ExecutorService executorService = Executors.newFixedThreadPool(topics.size());
        for (String topic : topics) {
            TopicPublishTask topicPublishTask = new TopicPublishTask(topicAdminClient, projectId, topic);
            threadMap.put(topic, topicPublishTask);
            executorService.submit(topicPublishTask);
        }

        while (true) {
            Scanner scanner = new Scanner(System.in);
            String req = scanner.nextLine();
            String topic = req.split(",")[1];
            String message = req.split(",")[0];
            threadMap.get(topic).messages.add(message);
        }
    }
}
