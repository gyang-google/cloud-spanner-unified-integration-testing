package com.google.cloud.testing;

import com.google.spanner.admin.instance.v1.InstanceConfig;
import com.google.spanner.executor.v1.AdminAction;
import com.google.spanner.executor.v1.CreateCloudInstanceAction;
import com.google.spanner.executor.v1.DeleteCloudBackupAction;
import com.google.spanner.executor.v1.DeleteCloudInstanceAction;
import com.google.spanner.executor.v1.DropCloudDatabaseAction;
import com.google.spanner.executor.v1.GetCloudInstanceConfigAction;
import com.google.spanner.executor.v1.ListCloudBackupsAction;
import com.google.spanner.executor.v1.ListCloudDatabasesAction;
import com.google.spanner.executor.v1.ListCloudInstanceConfigsAction;
import com.google.spanner.executor.v1.UpdateCloudDatabaseAction;
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
    SpannerAction action = SpannerAction.newBuilder()
        .setStart(StartTransactionAction.newBuilder().build()).setDatabasePath(databasePath)
        .build();
    return executor.executeAction(action).getStatus();
  }

  public static Status abandonTransaction(String databasePath, CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setFinish(
            FinishTransactionAction.newBuilder().setMode(FinishTransactionAction.Mode.ABANDON).build())
        .setDatabasePath(databasePath).build();
    return executor.executeAction(action).getStatus();
  }

  public static Status commitTransaction(String databasePath, CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setFinish(
            FinishTransactionAction.newBuilder().setMode(FinishTransactionAction.Mode.COMMIT).build())
        .setDatabasePath(databasePath).build();
    return executor.executeAction(action).getStatus();
  }

  public static Status executeInTransaction(String databasePath,
      Function<CloudExecutor, Status> function, CloudExecutor executor) {
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
    return KeySet.newBuilder()
        .addPoint(ValueList.newBuilder().addValue(Value.newBuilder().setStringValue(key))).build();
  }

  public static ReadResult readUsingIndex(String databasePath, String table, String index,
      KeySet keySet, List<String> columns, int limit, CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setRead(
        ReadAction.newBuilder().setTable(table).setIndex(index).setKeys(keySet)
            .addAllColumn(columns).setLimit(limit).build()).setDatabasePath(databasePath).build();
    StatusOrSpannerActionOutcome outcome = executor.executeAction(action);
    if (outcome.getStatus().equals(Status.OK)) {
      return outcome.getOutcome().getReadResult();
    } else {
      return null;
    }
  }

  public static ReadResult read(String databasePath, String table, KeySet keySet,
      List<String> columns, int limit, CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setRead(
        ReadAction.newBuilder().setTable(table).setKeys(keySet).addAllColumn(columns)
            .setLimit(limit).build()).setDatabasePath(databasePath).build();
    StatusOrSpannerActionOutcome outcome = executor.executeAction(action);
    if (outcome.getStatus().equals(Status.OK)) {
      return outcome.getOutcome().getReadResult();
    } else {
      return null;
    }
  }

  public static StatusOrSpannerActionOutcome GetInstanceConfigId(String configName,
      String projectId,
      CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setAdmin(AdminAction.newBuilder()
        .setGetCloudInstanceConfig(
            GetCloudInstanceConfigAction.newBuilder().setInstanceConfigId(configName)
                .setProjectId(projectId).build()).build()).build();
    return executor.executeAction(action);
  }

  public static StatusOrSpannerActionOutcome ListInstanceConfigId(String projectId,
      CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setAdmin(AdminAction.newBuilder()
        .setListInstanceConfigs(
            ListCloudInstanceConfigsAction.newBuilder()
                .setProjectId(projectId).build()).build()).build();
    return executor.executeAction(action);
  }

    public static StatusOrSpannerActionOutcome CreateCloudInstance(String projectId, String instanceId, String configId, int nodeCount,
        CloudExecutor executor) {
        SpannerAction action = SpannerAction.newBuilder().setAdmin(AdminAction.newBuilder()
            .setCreateCloudInstance(
                CreateCloudInstanceAction.newBuilder()
                    .setProjectId(projectId).setInstanceId(instanceId).setNodeCount(nodeCount).setInstanceConfigId(configId).build()).build()).build();
        return executor.executeAction(action);
    }

  public static StatusOrSpannerActionOutcome DeleteCloudInstance(String projectId, String instanceId, CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setAdmin(AdminAction.newBuilder()
        .setDeleteCloudInstance(
            DeleteCloudInstanceAction.newBuilder()
                .setProjectId(projectId).setInstanceId(instanceId).build()).build()).build();
    return executor.executeAction(action);
  }

    public static StatusOrSpannerActionOutcome ListCloudDatabases(String projectId, String instanceId, CloudExecutor executor) {
        SpannerAction action = SpannerAction.newBuilder().setAdmin(AdminAction.newBuilder()
            .setListCloudDatabases(
                ListCloudDatabasesAction.newBuilder()
                    .setProjectId(projectId).setInstanceId(instanceId).build()).build()).build();
        return executor.executeAction(action);
    }

  public static StatusOrSpannerActionOutcome DropCloudDatabase(String projectId, String instanceId, String databaseId, CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setAdmin(AdminAction.newBuilder()
        .setDropCloudDatabase(
            DropCloudDatabaseAction.newBuilder()
                .setProjectId(projectId).setInstanceId(instanceId).setDatabaseId(databaseId).build()).build()).build();
    return executor.executeAction(action);
  }

  public static StatusOrSpannerActionOutcome DisableCloudDatabaseDropProtection(String projectId, String instanceId, String databaseId, CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setAdmin(AdminAction.newBuilder()
        .setUpdateCloudDatabase(
            UpdateCloudDatabaseAction.newBuilder()
                .setProjectId(projectId).setInstanceId(instanceId).setDatabaseName(databaseId).setEnableDropProtection(false).build()).build()).build();
    return executor.executeAction(action);
  }

  public static StatusOrSpannerActionOutcome ListCloudBackup(String projectId, String instanceId, CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setAdmin(AdminAction.newBuilder()
        .setListCloudBackups(
            ListCloudBackupsAction.newBuilder()
                .setProjectId(projectId).setInstanceId(instanceId).build()).build()).build();
    return executor.executeAction(action);
  }

  public static StatusOrSpannerActionOutcome DeleteCloudBackup(String projectId, String instanceId, String backupId, CloudExecutor executor) {
    SpannerAction action = SpannerAction.newBuilder().setAdmin(AdminAction.newBuilder()
        .setDeleteCloudBackup(
            DeleteCloudBackupAction.newBuilder()
                .setProjectId(projectId).setInstanceId(instanceId).setBackupId(backupId).build()).build()).build();
    return executor.executeAction(action);
  }
}
