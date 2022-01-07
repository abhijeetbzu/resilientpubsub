package org.resilient;

import com.google.pubsub.v1.PublishRequest;

import java.util.concurrent.ConcurrentHashMap;

public class RequestExecutionInfoHolder {
    public ConcurrentHashMap<PublishRequest, StringBuilder> requestInfoMap = new ConcurrentHashMap<>();

    private void limiter() {
        System.out.println("----------------------");
    }

    public void print(PublishRequest publishRequest) {
        limiter();

        StringBuilder stringBuilder = requestInfoMap.get(publishRequest);
        System.out.println(stringBuilder.toString());

        limiter();
    }

    public synchronized void append(PublishRequest publishRequest, String text) {
        if (!requestInfoMap.containsKey(publishRequest)) requestInfoMap.put(publishRequest, new StringBuilder());
        StringBuilder stringBuilder = requestInfoMap.get(publishRequest);
        stringBuilder.append(text);
        stringBuilder.append("\n");
    }
}
