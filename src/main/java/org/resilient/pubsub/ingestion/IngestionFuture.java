package org.resilient.pubsub.ingestion;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import org.resilient.pubsub.utils.PubSubRequestFutureHolder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IngestionFuture implements Future<PublishResponse> {
    private Future<PublishResponse> apiFuture;
    private final PublishRequest publishRequest;
    private boolean isDone = false;

    public IngestionFuture(Future<PublishResponse> apiFuture, PublishRequest publishRequest) {
        this.apiFuture = apiFuture;
        this.publishRequest = publishRequest;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return apiFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return apiFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        if (isDone) return true;

        if (apiFuture.isDone()) {
            try {
                apiFuture.get();
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    private PublishResponse execute(Future<PublishResponse> future) throws ExecutionException, InterruptedException {
        PublishResponse publishResponse = future.get();
        isDone = true;
        PubSubRequestFutureHolder.futureMap.remove(publishRequest);
        return publishResponse;
    }

    private PublishResponse execute(Future<PublishResponse> future, long timeout, TimeUnit unit)
            throws ExecutionException, InterruptedException, TimeoutException {
        PublishResponse publishResponse = future.get(timeout, unit);
        isDone = true;
        PubSubRequestFutureHolder.futureMap.remove(publishRequest);
        return publishResponse;
    }


    public PublishResponse completeGet() throws ExecutionException, InterruptedException {
        isDone = true;
        return apiFuture.get();
    }

    @Override
    public PublishResponse get() throws InterruptedException, ExecutionException {
        while (PubSubRequestFutureHolder.futureMap.containsKey(publishRequest)) {
            try {
                return execute(apiFuture);
            } catch (ExecutionException e) {
                /*  execution exception while executing request
                    means updated future object will be added for this request soon by callback
                    so continue till we find new future object which have success response
                 */
                Future<PublishResponse> future = PubSubRequestFutureHolder.futureMap.get(publishRequest).get();
                if (future != this)
                    apiFuture = future;
            } catch (Exception e) {
                /*
                    something wrong happened with the request, will not get any updated future for this
                    so exit
                 */
                completeGet();
            }
        }
        return completeGet();
    }

    @Override
    public PublishResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        while (PubSubRequestFutureHolder.futureMap.containsKey(publishRequest)) {
            try {
                return execute(apiFuture, timeout, unit);
            } catch (ExecutionException e) {
                /*  execution exception while executing request
                    means updated future object will be added for this request soon by callback
                    so continue till we find new future object which have success response
                 */
                Future<PublishResponse> future = PubSubRequestFutureHolder.futureMap.get(publishRequest).get();
                if (future != this)
                    apiFuture = future;
            } catch (Exception e) {
                /*
                    something wrong happened with the request, will not get any updated future for this
                    so exit
                 */
                completeGet();
            }
        }
        return completeGet();
    }
}
