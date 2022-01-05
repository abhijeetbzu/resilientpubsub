package org.resilient;

import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.TopicName;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Demo {
    public static final CircuitBreakerFactory circuitBreakerFactory = new CircuitBreakerFactory();

    public static ResilientPublisher getResilientPublisher(ResilientPublisher fallbackPublisher,
                                                           String endpoint, String circuitBreakerName)
            throws IOException {
        PublisherComponentFactory publisherComponentFactory = new PublisherComponentFactory(endpoint);
        TopicAdminClientFactory topicAdminClientFactory = new TopicAdminClientFactory(publisherComponentFactory);
        PubSubIngestor pubSubIngestor = new PubSubIngestor(topicAdminClientFactory);
        return new ResilientPublisher(fallbackPublisher,
                pubSubIngestor, circuitBreakerFactory, circuitBreakerName);
    }

    public static ResilientPublisher getResilientPublisher(String endpoint, String circuitBreakerName)
            throws IOException {
        return getResilientPublisher(null, endpoint, circuitBreakerName);
    }

    public static void test(ExecutorService executorService,final ResilientPublisher resilientPublisher,
                            TopicName topicName) throws InterruptedException {
        int i = 1;
        while (i < 200) {
            String message = "Hello" + i + "!";
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Future<PublishResponse> publishResponseFuture = resilientPublisher.
                                publish(topicName, message);
                        //PublishResponse publishResponse = publishResponseFuture.get();
//                        System.out.println("Message '" + message + "': " + publishResponse.getMessageIdsCount());
                    } catch (Throwable e) {
                        System.out.println();
                    }
                }
            });
            if ((i % 30) == 0) TimeUnit.SECONDS.sleep(10);
            i++;
        }
    }

    public static void main(String[] args) throws Throwable {
        String endpointA = "asia-south2-pubsub.googleapis.com:443";
        String circuitBreakerNameA = "cbA";
        ResilientPublisher resilientPublisherA = getResilientPublisher(endpointA, circuitBreakerNameA);


        String endpointB = "asia-south-pubsub.googleapis.com:443";
        String circuitBreakerNameB = "cbB";
        ResilientPublisher resilientPublisherB = getResilientPublisher(resilientPublisherA,
                endpointB, circuitBreakerNameB);


        String projectId = "fk-sanbox-fdp-temp-1";
        String topic = "newone";
        TopicName topicName = TopicName.of(projectId, topic);

        ExecutorService executorService = Executors.newFixedThreadPool(20);
        int i = 1;
        while (i < 200) {
            String message = "Hello" + i + "!";
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Future<PublishResponse> publishResponseFuture = resilientPublisherB.
                                publish(topicName, message);
                        //PublishResponse publishResponse = publishResponseFuture.get();
//                        System.out.println("Message '" + message + "': " + publishResponse.getMessageIdsCount());
                    } catch (Throwable e) {
                        System.out.println();
                    }
                }
            });
            if ((i % 30) == 0) TimeUnit.SECONDS.sleep(10);
            i++;
        }

        System.out.println("done");
        endpointB = "asia-south1-pubsub.googleapis.com:443";
        ResilientPublisher resilientPublisherC = getResilientPublisher(resilientPublisherA,
                endpointB, circuitBreakerNameB);

        i = 201;
        while (i < 500) {
            String message = "Hello" + i + "!";
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Future<PublishResponse> publishResponseFuture = resilientPublisherC.
                                publish(topicName, message);
                        //PublishResponse publishResponse = publishResponseFuture.get();
//                        System.out.println("Message '" + message + "': " + publishResponse.getMessageIdsCount());
                    } catch (Throwable e) {
                        System.out.println();
                    }
                }
            });
            if ((i % 30) == 0) TimeUnit.SECONDS.sleep(10);
            i++;
        }


    }
}
