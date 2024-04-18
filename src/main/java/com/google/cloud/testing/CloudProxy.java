package com.google.cloud.testing;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import com.google.api.client.util.Preconditions;
import com.google.cloud.testing.CloudExecutor.OutcomeMerger;
import com.google.spanner.executor.v1.SpannerAsyncActionRequest;
import com.google.spanner.executor.v1.SpannerAsyncActionResponse;
import com.google.spanner.executor.v1.SpannerExecutorProxyGrpc;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class CloudProxy {
    private static final Logger logger = Logger.getLogger(CloudProxy.class.getName());
    private static final int deadlineInSeconds = 5;

    private final SpannerExecutorProxyGrpc.SpannerExecutorProxyStub stub;
    private Status status;

    private class AsyncActionState {
        public CountDownLatch finishLatch;
        public OutcomeMerger merger;
    }
    private HashMap<Integer, AsyncActionState> stateByActionId = new HashMap<Integer, AsyncActionState>();
    private CountDownLatch doneLatch;

    public CloudProxy(Channel channel) {
        this.stub = SpannerExecutorProxyGrpc.newStub(channel);
        this.doneLatch = new CountDownLatch(1);
    }

    public void AddTaskState(int actionId, CountDownLatch finishLatch, OutcomeMerger merger) {
        AsyncActionState state = new AsyncActionState();
        state.finishLatch = finishLatch;
        state.merger = merger;
        stateByActionId.put(actionId, state);
    }

    public boolean isTaskEmpty() {
        return stateByActionId.isEmpty();
    }

    public Status geStatus() {
        return status;
    }

    public void AwaitDone() throws InterruptedException {
        try {
            doneLatch.await();
        } catch (InterruptedException e) {
            logger.warning("Interrupted while waiting for latch");
            throw e;
        }
    }

    public StreamObserver<SpannerAsyncActionRequest> executeActionAsync() {
        StreamObserver<SpannerAsyncActionResponse> responseObserver = new StreamObserver<SpannerAsyncActionResponse>() {
            @Override
            public void onNext(SpannerAsyncActionResponse response) {
                logger.info("Received response: " + response.toString());
                Preconditions.checkState(response.hasOutcome(), "Received response without outcome");
                int id = response.getActionId();
                Preconditions.checkState(stateByActionId.containsKey(id), "Received response for unknown action id");
                stateByActionId.get(id).merger.MergeWith(response.getOutcome());
                if (response.getOutcome().hasStatus()) {
                    stateByActionId.get(id).finishLatch.countDown();
                    stateByActionId.remove(id);
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.warning("Error executing action: " + t.getMessage());
                status = Status.fromThrowable(t);
                // Notify all waiting actions
                for (AsyncActionState state : stateByActionId.values()) {
                    state.finishLatch.countDown();
                }
            }

            @Override
            public void onCompleted() {
                logger.info("Server closed the channel, action completed");
                doneLatch.countDown();
            }
        };
        return stub.withDeadlineAfter(deadlineInSeconds, java.util.concurrent.TimeUnit.SECONDS).executeActionAsync(responseObserver);
    }                                                                                                                                                                                 
}