package org.resilient;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;
import org.threeten.bp.Duration;

public class PublisherComponentFactory implements IngestionComponentFactory {
    private final String endpoint;

    public PublisherComponentFactory(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public BatchingSettings getBatchingSettings() {
        return BatchingSettings.newBuilder()
                .setDelayThreshold(Duration.ofSeconds(3))
                .setElementCountThreshold(2L)
                .setRequestByteThreshold(1000000L)
                .build();
    }

    @Override
    public RetrySettings getRetrySettings() {
        // Retry settings control how the publisher handles retry-able failures
        Duration initialRetryDelay = Duration.ofMillis(100); // default: 100 ms
        double retryDelayMultiplier = 2.0; // back off for repeated failures, default: 1.3
        Duration maxRetryDelay = Duration.ofSeconds(60); // default : 60 seconds
        Duration initialRpcTimeout = Duration.ofSeconds(1); // default: 5 seconds
        double rpcTimeoutMultiplier = 1.0; // default: 1.0
        Duration maxRpcTimeout = Duration.ofSeconds(600); // default: 600 seconds
        Duration totalTimeout = Duration.ofSeconds(600); // default: 600 seconds

        RetrySettings retrySettings =
                RetrySettings.newBuilder()
                        .setInitialRetryDelay(initialRetryDelay)
                        .setRetryDelayMultiplier(retryDelayMultiplier)
                        .setMaxRetryDelay(maxRetryDelay)
                        .setInitialRpcTimeout(initialRpcTimeout)
                        .setRpcTimeoutMultiplier(rpcTimeoutMultiplier)
                        .setMaxRpcTimeout(maxRpcTimeout)
                        .setTotalTimeout(totalTimeout)
                        .build();

        return retrySettings;
    }

    @Override
    public String getEndpoint() {
        return endpoint;
    }
}
