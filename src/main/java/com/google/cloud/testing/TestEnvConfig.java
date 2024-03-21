package com.google.cloud.testing;

import com.google.cloud.spanner.SpannerOptions;
import java.io.IOException;

/** Interface for TestEnvConfig. */
public interface TestEnvConfig {
  /** Returns the options to use to create the Cloud Spanner client for integration tests. */
  SpannerOptions spannerOptions();

  /** Custom setup. */
  void setUp() throws IOException;

  /** Custom tear down. */
  void tearDown();
}
