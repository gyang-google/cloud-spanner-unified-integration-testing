package com.google.cloud.testing;

import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;

import com.google.cloud.testing.CloudExecutor.StatusOrSpannerActionOutcome;
import com.google.spanner.executor.v1.FinishTransactionAction;
import com.google.spanner.executor.v1.KeySet;
import com.google.spanner.executor.v1.ReadAction;
import com.google.spanner.executor.v1.ReadResult;
import com.google.spanner.executor.v1.SpannerAction;
import com.google.spanner.executor.v1.StartTransactionAction;
import com.google.spanner.executor.v1.Value;
import com.google.spanner.executor.v1.ValueList;

import io.grpc.Status;

public class Utilities {
    private static final Logger logger = Logger.getLogger(Utilities.class.getName());

    public static Status startTransaction(String databasePath, CloudExecutor executor) {
        SpannerAction action = SpannerAction.newBuilder().setStart(StartTransactionAction.newBuilder().build()).setDatabasePath(databasePath).build();
        return executor.executeAction(action).getStatus();
    }

    public static Status abandonTransaction(String databasePath, CloudExecutor executor) {
        SpannerAction action = SpannerAction.newBuilder().setFinish(FinishTransactionAction.newBuilder().setMode(FinishTransactionAction.Mode.ABANDON).build()).setDatabasePath(databasePath).build();
        return executor.executeAction(action).getStatus();
    }

    public static Status commitTransaction(String databasePath, CloudExecutor executor) {
        SpannerAction action = SpannerAction.newBuilder().setFinish(FinishTransactionAction.newBuilder().setMode(FinishTransactionAction.Mode.COMMIT).build()).setDatabasePath(databasePath).build();
        return executor.executeAction(action).getStatus();
    }

    public static Status executeInTransaction(String databasePath, Function<CloudExecutor, Status> function, CloudExecutor executor) {
        Status status;
        status = startTransaction(databasePath, executor);
        if (!status.isOk()) {
            return status;
        }
        status = function.apply(executor);
        if (!status.isOk()) {
            logger.warning("Transaction failed");
        }
        status = abandonTransaction(databasePath, executor);
        return status;
    }

    public KeySet singlePointKey(String key) {
        return KeySet.newBuilder().addPoint(ValueList.newBuilder().addValue(Value.newBuilder().setStringValue(key))).build();
    }

    public static ReadResult readUsingIndex(String databasePath, String table, String index, KeySet keySet, List<String> columns, int limit, CloudExecutor executor) {
        SpannerAction action = SpannerAction.newBuilder().setRead(ReadAction.newBuilder().setTable(table).setIndex(index).setKeys(keySet).addAllColumn(columns).setLimit(limit).build()).setDatabasePath(databasePath).build();
        StatusOrSpannerActionOutcome outcome = executor.executeAction(action);
        if (outcome.getStatus().equals(Status.OK)) {
            return outcome.getOutcome().getReadResult();
        } else {
            return null;
        }
    }

    public static ReadResult read(String databasePath, String table, KeySet keySet, List<String> columns, int limit, CloudExecutor executor) {
        SpannerAction action = SpannerAction.newBuilder().setRead(ReadAction.newBuilder().setTable(table).setKeys(keySet).addAllColumn(columns).setLimit(limit).build()).setDatabasePath(databasePath).build();
        StatusOrSpannerActionOutcome outcome = executor.executeAction(action);
        if (outcome.getStatus().equals(Status.OK)) {
            return outcome.getOutcome().getReadResult();
        } else {
            return null;
        }
    }
}
