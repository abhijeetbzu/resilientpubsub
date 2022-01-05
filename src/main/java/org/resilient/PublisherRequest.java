package org.resilient;

import com.google.pubsub.v1.TopicName;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class PublisherRequest {
    private TopicName topicName;
    private String message;
}
