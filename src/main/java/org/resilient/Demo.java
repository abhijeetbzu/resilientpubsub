package org.resilient;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.TopicName;
import lombok.SneakyThrows;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class Demo {
    public static final CircuitBreakerFactory circuitBreakerFactory = new CircuitBreakerFactory();
    public static final ConcurrentHashMap<PublishRequest, AtomicReference<Future<PublishResponse>>> futureMap =
            new ConcurrentHashMap<>();

    public static final RequestExecutionInfoHolder requestExecutionInfoHolder = new RequestExecutionInfoHolder();

    public static void test(ExecutorService executorService, final ResilientPublisherFactory resilientPublisherFactory,
                            TopicName topicName, int start, int end) throws InterruptedException {
        while (start <= end) {
            String message = "Hello" + start + "!";
            executorService.submit(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    ResilientPublisher activePublisher = resilientPublisherFactory.getActivePublisher();
                    PublishRequest publishRequest = activePublisher.getPublishRequest(topicName, message);
                    try {
                        Future<PublishResponse> publishResponseFuture = activePublisher.
                                publish(publishRequest, topicName);
                        requestExecutionInfoHolder.append(publishRequest,
                                "Received future object successfully. Executing get()..");
                        PublishResponse publishResponse = publishResponseFuture.get();
                        requestExecutionInfoHolder.append(publishRequest,
                                "Message '" + message + "': " + publishResponse.getMessageIdsCount());
                    } catch (Exception e) {
                        requestExecutionInfoHolder.append(publishRequest,
                                "Exception while pushing:  " + e.getMessage());
                    }
                    requestExecutionInfoHolder.print(publishRequest);
                }
            });
            if ((start % 30) == 0) TimeUnit.SECONDS.sleep(10);
            start++;
        }
    }

    public static void main(String[] args) throws Throwable {
        ResilientPublisherFactory resilientPublisherFactory = new ResilientPublisherFactory(circuitBreakerFactory);

        String endpointA = "asia-south2-pubsub.googleapis.com:443";
        String circuitBreakerNameA = "cbA";
        ResilientPublisher resilientPublisherA = resilientPublisherFactory.
                getResilientPublisher(endpointA, circuitBreakerNameA);


        String endpointB = "asia-south-pubsub.googleapis.com:443";
        String circuitBreakerNameB = "cbB";
        ResilientPublisher resilientPublisherB = resilientPublisherFactory.
                getResilientPublisher(resilientPublisherA, endpointB, circuitBreakerNameB);
        resilientPublisherFactory.setPrimaryPublisher(resilientPublisherB);


        String projectId = "fk-sanbox-fdp-temp-1";
        String topic = "newone";
        TopicName topicName = TopicName.of(projectId, topic);

        ExecutorService executorService = Executors.newFixedThreadPool(20);
        test(executorService, resilientPublisherFactory, topicName, 0, 200);

        endpointB = "asia-south1-pubsub.googleapis.com:443";
        ResilientPublisher resilientPublisherC = resilientPublisherFactory.getResilientPublisher(resilientPublisherA,
                endpointB, circuitBreakerNameB);
        resilientPublisherFactory.setPrimaryPublisher(resilientPublisherC);
        System.out.println("Endpoint Up");

        test(executorService, resilientPublisherFactory, topicName, 201, 500);
    }
}
