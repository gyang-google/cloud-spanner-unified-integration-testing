package com.google.cloud.testing;

import com.google.cloud.spanner.SpannerOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import com.google.api.client.util.Preconditions;

import io.grpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc;

public class TestDriver {
    public static final String PROXY_TYPE_PROPERTY_NAME = "proxy_type";
    public static final String PROXY_TYPE = System.getProperty(PROXY_TYPE_PROPERTY_NAME, null);

    public static final String ARTIFACT_PATH_PROPERTY_NAME = "artifact_path";
    public static final String ARTIFACT_PATH = System.getProperty(ARTIFACT_PATH_PROPERTY_NAME, null);

    private static final Logger logger = Logger.getLogger(TestDriver.class.getName());

    private CountDownLatch startLatch;
    private final int proxyPort = 5431;
    private final SpannerOptions options;
    private final int numStartAttempts = 3;
    private final int startTimeoutSeconds = 30;
    private Thread proxyThread;
    private Process proxy;
    private Status status;
    
    public TestDriver(SpannerOptions options) {
        this.options = options;
    }
    
    public Status Setup() {
        logger.info("Setting up test environment");
        if (proxyThread == null) {
            Runnable runnable = () -> { RunProxy(); };
            proxyThread = new Thread(runnable);
            proxyThread.start();
        }
        startLatch = new CountDownLatch(1);
        try {
            startLatch.await();
        } catch (InterruptedException e) {
            return Status.DEADLINE_EXCEEDED.withDescription("Timeout waiting for proxy to start: " + e.getMessage());
        }
        return status;
    }

    private void RunProxy() {
        boolean proxyStarted = false;
        for (int i = 0; i < numStartAttempts; i++) {
            if (executeArtifact()) {
                proxyStarted = true;
                break;
            } else {
                logger.warning("Failed to start proxy, attempt " + i);
            }
        }
        synchronized (this) {
            startLatch.countDown();
        }
        if (proxyStarted) {
            try {
                int state = proxy.waitFor();
                if (state != 0) {
                    logger.severe("Proxy exited with non-zero status: " + state);
                }
            } catch (InterruptedException e) {
                logger.warning("Interrupted while waiting for proxy to exit: " + e.getMessage());
            }
        }
    }

    private boolean executeArtifact() {
        Preconditions.checkState(startLatch.getCount() > 0, "startLatch already counted down");

        List<String> command = new ArrayList<>();
        if (PROXY_TYPE.equals("java")) {
            command.add("java");
            command.add("-jar");
        }
        command.add(ARTIFACT_PATH);
        command.add("--proxy_port");
        command.add("" + proxyPort);

        command.add("--spanner_endpoint");
        command.add("" + options.getEndpoint());
        // command.add("--cert");
        // command.add("cert.pem");

        ProcessBuilder builder = new ProcessBuilder(command);
        try {
            proxy = builder.inheritIO().start();
        } catch (IOException e) {
            status = Status.fromThrowable(e);
            logger.warning("Failed to start proxy: " + e.getMessage());
            return false;
        }
        logger.info("proxy started");

        boolean started = proxyIsHealthy();
        if (!started) {
            logger.warning("Proxy is not healthy, stopping");
            proxy.destroy();
            logger.info("proxy destroyed");
            // Sleep briefly before trying to start the proxy again.
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.warning("Interrupted while sleeping: " + e.getMessage());
            }
        }
        return started;
    }

    private boolean proxyIsHealthy() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", proxyPort).usePlaintext().build();
        long deadline = System.currentTimeMillis() + startTimeoutSeconds * 1000;
        String error = "";
        do {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                logger.warning("Interrupted while sleeping: " + e.getMessage());
            }

            final HealthGrpc.HealthBlockingStub healthBlockingStub = HealthGrpc.newBlockingStub(channel);
            final HealthCheckRequest healthRequest = HealthCheckRequest.getDefaultInstance();
            HealthCheckResponse response = healthBlockingStub.withDeadlineAfter(10, java.util.concurrent.TimeUnit.SECONDS).check(healthRequest);
            if (response.getStatus().equals(ServingStatus.SERVING)) {
                error = "";
                break;
            } else {
                error = "Server not ready yet";
            }
            logger.warning("Health check failed: " + error);
        } while (System.currentTimeMillis() <= deadline);
        if (!error.isEmpty()) {
            status = Status.INTERNAL.withDescription("Proxy failed to respond: " + error);
            logger.severe(status.getDescription());
            return false;
        }
        logger.info("Proxy is healthy");
        return true;
    }

    public CloudExecutor NewExecutor() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", proxyPort).usePlaintext().build();
        return new CloudExecutor(channel);
    }
}
    