package com.google.cloud.testing;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import com.google.api.client.util.Preconditions;
import com.google.spanner.executor.v1.QueryResult;
import com.google.spanner.executor.v1.ReadResult;
import com.google.spanner.executor.v1.SpannerAction;
import com.google.spanner.executor.v1.SpannerActionOutcome;
import com.google.spanner.executor.v1.SpannerAsyncActionRequest;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class CloudExecutor {
    private static final Logger logger = Logger.getLogger(CloudExecutor.class.getName());
    private final CloudProxy proxy;
    private final StreamObserver<SpannerAsyncActionRequest> requestObserver;
    private Boolean isDone = false;
    private Status status = Status.OK;

    private int nextActionId = 0;

    public CloudExecutor(Channel channel) {
        this.proxy = new CloudProxy(channel);
        this.requestObserver = proxy.executeActionAsync();
    }

    public class StatusOrSpannerActionOutcome {
        private Status status = null;
        private SpannerActionOutcome outcome = null;

        public StatusOrSpannerActionOutcome(Status status) {
            this.status = status;
        }

        public StatusOrSpannerActionOutcome(SpannerActionOutcome outcome) {
            this.status = Status.OK;
            this.outcome = outcome;
        } 

        public Status getStatus() {
            return status;
        }

        public SpannerActionOutcome getOutcome() {
            return outcome;
        }
    }
    

    public StatusOrSpannerActionOutcome executeAction(SpannerAction action) {
        if (!status.isOk()) {
            return new StatusOrSpannerActionOutcome(status);
        }
        SpannerAsyncActionRequest request = SpannerAsyncActionRequest.newBuilder().setAction(action).setActionId(nextActionId).build();
        CountDownLatch finishLatch = new CountDownLatch(1);
        OutcomeMerger merger = new OutcomeMerger(action);
        proxy.AddTaskState(nextActionId, finishLatch, merger);
        nextActionId++;
        try {
            requestObserver.onNext(request);
            // Wait for the action to finish
            finishLatch.await();
            // Check the proxy status
            status = proxy.geStatus();
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            status = Status.fromThrowable(e);
        } catch (InterruptedException e) {
            logger.warning("Interrupted while waiting for latch");
            requestObserver.onError(e);
            status = Status.fromThrowable(e);
        }

        if (!status.isOk()) {
            return new StatusOrSpannerActionOutcome(status);
        } else {
            return new StatusOrSpannerActionOutcome(merger.GetMerged());
        }
    }

    public Status Done() {
        if (isDone) {
            return Status.FAILED_PRECONDITION.withDescription("Done() has already been called");
        }
        if (!proxy.isTaskEmpty()) {
            return Status.FAILED_PRECONDITION.withDescription("Done() is called when there are still pending tasks");
        }
        isDone = true;
        requestObserver.onCompleted();
        try {
            proxy.AwaitDone();
        } catch (InterruptedException e) {
            return Status.fromThrowable(e);
        }       
        return status;
    }

    public class OutcomeMerger {
        private SpannerActionOutcome.Builder mergedOutcome;
        private SpannerAction action;

        public OutcomeMerger(SpannerAction action) {
            this.action = action;
            this.mergedOutcome = SpannerActionOutcome.newBuilder();
        }

        public void MergeWith(SpannerActionOutcome outcome) {
            logger.info("Merging outcome: " + outcome.toString());
            synchronized (this) {
                if (outcome.hasStatus()) {
                    if (mergedOutcome.hasStatus()) {
                       Preconditions.checkState(mergedOutcome.getStatus().getCode() == Status.OK.getCode().value(), "Received two outcomes with non-OK status");
                    }
                    mergedOutcome.setStatus(outcome.getStatus());
                }
                if (outcome.hasCommitTime()) {
                    if (mergedOutcome.hasCommitTime()) {
                        Preconditions.checkState(mergedOutcome.getCommitTime().equals(outcome.getCommitTime()), "Received two outcomes with different timestamps");
                    } else {
                        mergedOutcome.setCommitTime(outcome.getCommitTime());
                    }
                }
                if (outcome.hasReadResult()) {
                    MergeWithReadResult(outcome.getReadResult());
                }
                if (outcome.hasQueryResult()) {
                    MergeWithQueryResult(outcome.getQueryResult());
                }
                if (outcome.hasTransactionRestarted()) {
                    mergedOutcome.setTransactionRestarted(outcome.getTransactionRestarted());
                }
                if (outcome.hasBatchTxnId()) {
                    mergedOutcome.setBatchTxnId(outcome.getBatchTxnId());
                }
                Preconditions.checkState(mergedOutcome.getDbPartitionCount() == 0, "Received two outcomes with partition tokens.");
                for (int i = 0; i < outcome.getDbPartitionCount(); i++) {
                    mergedOutcome.addDbPartition(outcome.getDbPartition(i));
                }
                if (outcome.hasAdminResult()) {
                    Preconditions.checkState(!mergedOutcome.hasAdminResult(), "Received two outcomes with admin results");
                    mergedOutcome.setAdminResult(outcome.getAdminResult());
                }
                if (outcome.getDmlRowsModifiedCount() > 0) {
                    Preconditions.checkState(mergedOutcome.getDmlRowsModifiedCount() == 0, "Received multiple rows modified for an action " + action);
                    for (int i = 0; i < outcome.getDmlRowsModifiedCount(); i++) {
                        mergedOutcome.addDmlRowsModified(outcome.getDmlRowsModified(i));
                    }
                }
            }
        }

        public void MergeWithReadResult(ReadResult readResult) {
            Preconditions.checkState(!mergedOutcome.hasQueryResult(), "Received outcomes with both ReadResult and QueryResult");
            Preconditions.checkState(mergedOutcome.getReadResult().getTable().equals(readResult.getTable()), "Received outcomes with different read table names");
            mergedOutcome.getReadResultBuilder().setTable(readResult.getTable());
            if (mergedOutcome.getReadResult().hasIndex()) {
                Preconditions.checkState(mergedOutcome.getReadResult().getIndex().equals(readResult.getIndex()), "Received outcomes with different read index names");
            } else {
                mergedOutcome.getReadResultBuilder().setIndex(readResult.getIndex());
            }
            for (int i = 0; i < readResult.getRowCount(); i++) {
                mergedOutcome.getReadResultBuilder().addRow(readResult.getRow(i));
            }
            if (readResult.hasRowType()) {
                Preconditions.checkState(mergedOutcome.getReadResult().getRowType().equals(readResult.getRowType()), "Received outcomes with different read row types");
            } else {
                mergedOutcome.getReadResultBuilder().setRowType(readResult.getRowType());
            }
        }

        public void MergeWithQueryResult(QueryResult queryResult) {
            Preconditions.checkState(!mergedOutcome.hasReadResult(), "Received outcomes with both ReadResult and QueryResult");
            for (int i = 0; i < queryResult.getRowCount(); i++) {
                mergedOutcome.getQueryResultBuilder().addRow(queryResult.getRow(i));
            }
            if (queryResult.hasRowType()) {
                Preconditions.checkState(mergedOutcome.getQueryResult().getRowType().equals(queryResult.getRowType()), "Received outcomes with different query row types");
            } else {
                mergedOutcome.getQueryResultBuilder().setRowType(queryResult.getRowType());
            }          
        }

        SpannerActionOutcome GetMerged() {
            synchronized (this) {
                Preconditions.checkState(mergedOutcome.hasStatus(), "Merged outcome has no status. This likely means that the action has not finished execution yet.");
                return mergedOutcome.build();
            }
        }
    }
}
