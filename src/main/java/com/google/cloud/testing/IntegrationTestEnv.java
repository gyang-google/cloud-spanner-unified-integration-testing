package com.google.cloud.testing;

import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseInfo.DatabaseField;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.testing.EmulatorSpannerHelper;
import com.google.cloud.spanner.testing.RemoteSpannerHelper;
import com.google.cloud.testing.CloudExecutor.StatusOrSpannerActionOutcome;
import com.google.common.collect.Iterators;
import com.google.spanner.admin.database.v1.Backup;
import com.google.spanner.admin.database.v1.Database;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import com.google.spanner.admin.instance.v1.InstanceConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.rules.ExternalResource;

/**
 * JUnit 4 test rule that provides access to a Cloud Spanner instance to use for tests, and allows
 * uniquely named (per {@code IntegrationTestEnv} instance) test databases to be created within that
 * instance. An existing instance can be used by naming it in the {@link #TEST_INSTANCE_PROPERTY}
 * property; if the property is not set, an instance will be created and destroyed by the rule.
 *
 * <p>This class is normally used as a {@code @ClassRule}.
 */
public class IntegrationTestEnv extends ExternalResource {

  /**
   * Names a property that provides the class name of the {@link TestEnvConfig} to use.
   */
  public static final String TEST_ENV_CONFIG_CLASS_NAME = "spanner.testenv.config.class";

  public static final String CONFIG_CLASS = System.getProperty(TEST_ENV_CONFIG_CLASS_NAME, null);

  /**
   * Names a property that, if set, identifies an existing Cloud Spanner instance to use for tests.
   */
  public static final String TEST_INSTANCE_PROPERTY = "spanner.testenv.instance";

  public static final String MAX_CREATE_INSTANCE_ATTEMPTS =
      "spanner.testenv.max_create_instance_attempts";

  // Pattern for a database name: projects/<project>/instances/<instance>
  public static final Pattern INSTANCE_NAME =
      Pattern.compile(
          "projects/([A-Za-z0-9-_]+)/instances/([A-Za-z0-9-_]+)");

  private static final Logger logger = Logger.getLogger(IntegrationTestEnv.class.getName());

  private TestEnvConfig config;
  private boolean isOwnedInstance;
  private final boolean alwaysCreateNewInstance;
  private String projectId;
  private String instanceId;

  private CloudExecutor executor;

  public IntegrationTestEnv() {
    this(false);
  }

  public IntegrationTestEnv(final boolean alwaysCreateNewInstance) {
    this.alwaysCreateNewInstance = alwaysCreateNewInstance;
  }

  public CloudExecutor getExecutor() {
    checkInitialized();
    return executor;
  }

  @SuppressWarnings("unchecked")
  protected void initializeConfig()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    if (CONFIG_CLASS == null) {
      throw new NullPointerException("Property " + TEST_ENV_CONFIG_CLASS_NAME + " needs to be set");
    }
    Class<? extends TestEnvConfig> configClass;
    configClass = (Class<? extends TestEnvConfig>) Class.forName(CONFIG_CLASS);
    config = configClass.newInstance();
  }

  @Override
  protected void before() throws Throwable {
    this.initializeConfig();
    this.config.setUp();

    SpannerOptions options = config.spannerOptions();
    String instancePath = System.getProperty(TEST_INSTANCE_PROPERTY, "");
    if (!instancePath.isEmpty() && !alwaysCreateNewInstance) {
      projectId = extractProjectIdFromInstancePath(instancePath);
      instanceId = extractInstanceIdFromInstancePath(instancePath);
      isOwnedInstance = false;
      logger.log(Level.INFO, "Using existing test instance: {0}", instancePath);
    } else {
      projectId = config.spannerOptions().getProjectId();
      instanceId = String.format("test-instance-%08d", new Random().nextInt(100000000));
      isOwnedInstance = true;
    }
    TestDriver driver = new TestDriver(options);
    Status status = driver.Setup();
    if (!status.isOk()) {
      logger.log(Level.SEVERE, "Failed to setup test driver", status.getCause());
      throw status.asException();
    }
    executor = driver.NewExecutor();
    logger.log(Level.FINE, "Test env endpoint is {0}", options.getHost());
    if (isOwnedInstance) {
      initializeInstance();
    } else {
      cleanUpOldDatabases();
    }
  }

  @Override
  protected void after() {
    cleanUpInstance();
    this.config.tearDown();
  }

  private void initializeInstance() throws Exception {
    StatusOrSpannerActionOutcome outcome;
    String instanceConfigName = null;
    try {
      outcome = Utilities.GetInstanceConfigId(projectId, "regional-us-central1", executor);
      if (outcome.getStatus().isOk()) {
        instanceConfigName = outcome.getOutcome().getAdminResult().getInstanceConfigResponse()
            .getInstanceConfig().getName();
      }
    } catch (Throwable ignore) {
      outcome = Utilities.ListInstanceConfigId(projectId, executor);
      if (outcome.getStatus().isOk()) {
        List<InstanceConfig> configList = outcome.getOutcome()
            .getAdminResult().getInstanceConfigResponse().getListedInstanceConfigsList();
        if (!configList.isEmpty()) {
          instanceConfigName = configList.get(0).getName();
        }
      }
    }
    checkState(instanceConfigName != null, "No instance configs found");
    logger.log(Level.FINE, "Creating instance using config {0}", instanceConfigName);
    int maxAttempts = 25;
    try {
      maxAttempts =
          Integer.parseInt(
              System.getProperty(MAX_CREATE_INSTANCE_ATTEMPTS, String.valueOf(maxAttempts)));
    } catch (NumberFormatException ignore) {
      // Ignore and fall back to the default.
    }
    ExponentialBackOff backOff =
        new ExponentialBackOff.Builder()
            .setInitialIntervalMillis(5_000)
            .setMaxIntervalMillis(500_000)
            .setMultiplier(2.0)
            .build();
    int attempts = 0;
    while (true) {
      outcome = Utilities.CreateCloudInstance(projectId, instanceId, instanceConfigName, 1,
          executor);
      if (!outcome.getStatus().isOk()) {
        Status status = outcome.getStatus();
        if (attempts < maxAttempts && isRetryableResourceExhaustedException(status)) {
          attempts++;
          // The Math.max(...) prevents Backoff#STOP (=-1) to be used as the sleep value.
          //noinspection BusyWait
          Thread.sleep(Math.max(backOff.getMaxIntervalMillis(), backOff.nextBackOffMillis()));
          continue;
        }
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INTERNAL,
            String.format(
                "Could not create test instance and giving up after %d attempts: %s",
                attempts, status.getDescription()),
            status.getCause());
      }
      logger.log(Level.INFO, "Created test instance: {0}",
          outcome.getOutcome().getAdminResult().getInstanceResponse().getInstance().getName());
      break;
    }
  }

  static boolean isRetryableResourceExhaustedException(Status status) {
    if (status.getCode() != Code.RESOURCE_EXHAUSTED) {
      return false;
    }
    return status
        .getDescription()
        .contains(
            "Quota exceeded for quota metric 'Instance create requests' and limit 'Instance create requests per minute'")
        || status.getDescription().matches(".*cannot add \\d+ nodes in region.*");
  }

  private void cleanUpOldDatabases() {
    long OLD_DB_THRESHOLD_SECS = TimeUnit.SECONDS.convert(6L, TimeUnit.HOURS);
    Timestamp currentTimestamp = Timestamp.now();
    int numDropped = 0;
    String TEST_DB_REGEX = "(testdb_(.*)_(.*))|(mysample-(.*))";

    logger.log(Level.INFO, "Dropping old test databases from {0}", instanceId);
    StatusOrSpannerActionOutcome outcome = Utilities.ListCloudDatabases(projectId, instanceId,
        executor);
    if (!outcome.getStatus().isOk()) {
      logger.log(Level.SEVERE, "Failed to list test database", outcome.getStatus().getCause());
    }
    for (Database db : outcome.getOutcome().getAdminResult().getDatabaseResponse()
        .getListedDatabasesList()) {
      long timeDiff = currentTimestamp.getSeconds() - db.getCreateTime().getSeconds();
      // Delete all databases which are more than OLD_DB_THRESHOLD_SECS seconds old.
      if ((db.getName().matches(TEST_DB_REGEX))
          && (timeDiff > OLD_DB_THRESHOLD_SECS)) {
        logger.log(Level.INFO, "Dropping test database {0}", db.getName());
        if (db.getEnableDropProtection()) {
          Status status = Utilities.DisableCloudDatabaseDropProtection(projectId, instanceId,
              db.getName(), executor).getStatus();
          if (!status.isOk()) {
            logger.log(Level.SEVERE,
                "Failed to disable drop protection for test database " + db.getName(),
                status.getCause());
            continue;
          }
        }
        Status status = Utilities.DropCloudDatabase(projectId, instanceId, db.getName(), executor)
            .getStatus();
        if (!status.isOk()) {
          logger.log(Level.SEVERE, "Failed to drop test database " + db.getName(),
              status.getCause());
        } else {
          ++numDropped;
        }
      }
    }
    logger.log(Level.INFO, "Dropped {0} test database(s)", numDropped);
  }

  private void cleanUpInstance() {
    if (isOwnedInstance) {
      // Delete the instance, which implicitly drops all databases in it.
      if (!EmulatorSpannerHelper.isUsingEmulator()) {
        // Backups must be explicitly deleted before the instance may be deleted.
        logger.log(
            Level.FINE, "Deleting backups on test instance {0}", instanceId);
        StatusOrSpannerActionOutcome outcome = Utilities.ListCloudBackup(projectId, instanceId,
            executor);
        if (!outcome.getStatus().isOk()) {
          logger.log(Level.SEVERE, "Failed to list backups", outcome.getStatus().getCause());
        }
        for (Backup backup : outcome.getOutcome().getAdminResult().getBackupResponse()
            .getListedBackupsList()) {
          logger.log(Level.FINE, "Deleting backup {0}", backup.getName());
          Status status = Utilities.DeleteCloudBackup(projectId, instanceId, backup.getName(),
                  executor)
              .getStatus();
          if (!status.isOk()) {
            logger.log(Level.SEVERE, "Failed to delete backup " + backup.getName(),
                status.getCause());
          }
        }
      }
      logger.log(Level.INFO, "Deleting test instance {0}", instanceId);
      Status status = Utilities.DeleteCloudInstance(projectId, instanceId, executor)
          .getStatus();
      if (!status.isOk()) {
        logger.log(Level.SEVERE, "Failed to delete test instance " + instanceId,
            status.getCause());
      } else {
        logger.log(Level.INFO, "Deleted test instance {0}", instanceId);
      }
    }
    executor.Done();
  }

  private void checkInitialized() {
    checkState(executor != null, "Setup has not completed successfully");
  }

  private String extractProjectIdFromInstancePath(String instancePath) {
    Matcher matcher = INSTANCE_NAME.matcher(instancePath);
    if (!matcher.matches()) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Bad instance path: " + instancePath);
    }
    return matcher.group(1);
  }

  private String extractInstanceIdFromInstancePath(String instancePath) {
    Matcher matcher = INSTANCE_NAME.matcher(instancePath);
    if (!matcher.matches()) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Bad instance path: " + instancePath);
    }
    return matcher.group(2);
  }
}
