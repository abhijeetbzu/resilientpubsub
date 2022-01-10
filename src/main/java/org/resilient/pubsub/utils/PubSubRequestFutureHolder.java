package org.resilient.pubsub.utils;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class PubSubRequestFutureHolder {
    public static ConcurrentHashMap<PublishRequest, AtomicReference<Future<PublishResponse>>> futureMap =
            new ConcurrentHashMap<>();
}
