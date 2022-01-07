package org.resilient;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;

import java.util.concurrent.*;

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
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        if (apiFuture.isDone()) {
            try {
                apiFuture.get();
                return true;
            } catch (Exception e) {
                return isDone;
            }
        }
        return false;
    }

    private PublishResponse execute(Future<PublishResponse> future) throws ExecutionException, InterruptedException {
        PublishResponse publishResponse = future.get();
        isDone = true;
        Demo.futureMap.remove(publishRequest);
        return publishResponse;
    }

    public PublishResponse completeGet() throws ExecutionException, InterruptedException {
        isDone = true;
        return apiFuture.get();
    }

    @Override
    public PublishResponse get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return null;
        }
    }

    @Override
    public PublishResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        while (Demo.futureMap.containsKey(publishRequest)) {
            try {
                return execute(apiFuture);
            } catch (ExecutionException e) {
                /*  execution exception while executing request
                    means updated future object will be added for this request soon by callback
                    so continue till we find new future object which have success response
                 */
                apiFuture = Demo.futureMap.get(publishRequest).get();
            } catch (Exception e) {
                /*
                    something wrong happened with the request, will not get any updated future for this
                    so exit
                 */

                System.out.println(publishRequest.getMessages(0).getData().toStringUtf8()+"....");
                return completeGet();
            }
        }
        return completeGet();
    }
}
