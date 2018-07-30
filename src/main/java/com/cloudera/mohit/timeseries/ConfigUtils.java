package com.cloudera.mohit.timeseries;

import com.typesafe.config.Config;

public class ConfigUtils {
  public static void assertConfig(Config config, String key) {
    if (!config.hasPath(key)) {
      throw new RuntimeException("Missing required property [" + key + "]");
    }
  }
}
