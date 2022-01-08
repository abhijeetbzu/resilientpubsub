package org.resilient.pubsub.factory;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;

import java.io.IOException;

public class TopicAdminClientFactory implements IngestionClientFactory {
    private final TopicAdminClient topicAdminClient;

    public TopicAdminClientFactory(IngestionComponentFactory ingestionComponentFactory) throws IOException {
        topicAdminClient = prepare(ingestionComponentFactory);
    }

    private TopicAdminClient prepare(IngestionComponentFactory ingestionComponentFactory) throws IOException {
        BatchingSettings batchingSettings = ingestionComponentFactory.getBatchingSettings();
        RetrySettings retrySettings = ingestionComponentFactory.getRetrySettings();
        String endpoint = ingestionComponentFactory.getEndpoint();

        InstantiatingExecutorProvider instantiatingExecutorProvider = InstantiatingExecutorProvider
                .newBuilder()
                .setExecutorThreadCount(10)
                .build();

        TopicAdminSettings.Builder topicAdminSettingsBuilder = TopicAdminSettings.newBuilder();
        topicAdminSettingsBuilder.getStubSettingsBuilder()
                .setBackgroundExecutorProvider(instantiatingExecutorProvider)
                .setEndpoint(endpoint);
        topicAdminSettingsBuilder.publishSettings()
                .setRetrySettings(retrySettings)
                .setBatchingSettings(batchingSettings);

        TopicAdminSettings topicAdminSettings = topicAdminSettingsBuilder.build();

        return TopicAdminClient.create(topicAdminSettings);
    }

    @Override
    public TopicAdminClient getTopicAdminClient() {
        return topicAdminClient;
    }

    @Override
    public Publisher getPublisher() {
        return null;
    }
}
