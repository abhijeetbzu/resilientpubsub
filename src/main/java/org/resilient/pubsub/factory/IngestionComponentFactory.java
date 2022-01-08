package org.resilient.pubsub.factory;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;

public interface IngestionComponentFactory {
    BatchingSettings getBatchingSettings();

    RetrySettings getRetrySettings();

    String getEndpoint();
}
