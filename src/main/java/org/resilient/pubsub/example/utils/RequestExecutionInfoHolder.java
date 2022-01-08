package org.resilient.pubsub.example.utils;

import com.google.pubsub.v1.PublishRequest;

import java.util.concurrent.ConcurrentHashMap;

public class RequestExecutionInfoHolder {
    public ConcurrentHashMap<PublishRequest, StringBuilder> requestInfoMap = new ConcurrentHashMap<>();

    public void print(PublishRequest publishRequest) {
        StringBuilder stringBuilder = requestInfoMap.get(publishRequest);
        System.out.println(stringBuilder);
    }

    private String limiter() {
        return "----------------------";
    }

    public synchronized void append(PublishRequest publishRequest, String text) {
        if (!requestInfoMap.containsKey(publishRequest)) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(limiter());
            stringBuilder.append("\n");
            requestInfoMap.put(publishRequest, stringBuilder);
        }
        StringBuilder stringBuilder = requestInfoMap.get(publishRequest);
        stringBuilder.append(text);
        stringBuilder.append("\n");
    }

    public void close(PublishRequest publishRequest) {
        StringBuilder stringBuilder = requestInfoMap.get(publishRequest);
        stringBuilder.append(limiter());
        stringBuilder.append("\n");
    }
}
