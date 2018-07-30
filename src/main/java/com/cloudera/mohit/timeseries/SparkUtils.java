/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.mohit.timeseries;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;

public class SparkUtils {
  private static Logger LOG = LoggerFactory.getLogger(SparkUtils.class);
  public static final String APPLICATION_NAME_PROPERTY = "application.name";
  public static final String BATCH_MILLISECONDS_PROPERTY = "application.batch.milliseconds";
  public static final String NUM_EXECUTORS_PROPERTY = "application.executors";
  public static final String NUM_EXECUTOR_CORES_PROPERTY = "application.executor.cores";
  public static final String EXECUTOR_MEMORY_PROPERTY = "application.executor.memory";
  public static final String SPARK_CONF_PROPERTY_PREFIX = "application.spark.conf";
  public static final String SPARK_SESSION_ENABLE_HIVE_SUPPORT = "application.hive.enabled";

  public static synchronized SparkSession getSparkSession(Config config) {
    SparkConf sparkConf = getSparkConfiguration(config);
    if (!sparkConf.contains("spark.master")) {
      LOG.warn("Spark master not provided, instead using local mode");
      sparkConf.setMaster("local[*]");
    }

    if (!sparkConf.contains("spark.app.name")) {
      LOG.warn("Spark application name not provided, instead using empty string");
      sparkConf.setAppName("");
    }

    SparkSession.Builder sparkSessionBuilder = SparkSession.builder();
    if (!config.hasPath(SPARK_SESSION_ENABLE_HIVE_SUPPORT) ||
        config.getBoolean(SPARK_SESSION_ENABLE_HIVE_SUPPORT)) {
      sparkSessionBuilder.enableHiveSupport();
    }

    return sparkSessionBuilder.config(sparkConf).getOrCreate();
  }

  private static SparkConf getSparkConfiguration(Config config) {
    SparkConf sparkConf = new SparkConf();

    if (config.hasPath(APPLICATION_NAME_PROPERTY)) {
      String applicationName = config.getString(APPLICATION_NAME_PROPERTY);
      sparkConf.setAppName(applicationName);
    }

    // Dynamic allocation should not be used for Spark Streaming jobs because the latencies
    // of the resource requests are too long.
    sparkConf.set("spark.dynamicAllocation.enabled", "false");
    // Spark Streaming back-pressure helps automatically tune the size of the micro-batches so
    // that they don't breach the micro-batch length.
    sparkConf.set("spark.streaming.backpressure.enabled", "true");
    // Rate limit the micro-batches when using Apache Kafka to 2000 records per Kafka topic partition
    // per second. Without this we could end up with arbitrarily large initial micro-batches
    // for existing topics.
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2000");
    // Override the Spark SQL shuffle partitions with the default number of cores. Otherwise
    // the default is typically 200 partitions, which is very high for micro-batches.
    sparkConf.set("spark.sql.shuffle.partitions", "2");
    // Override the caching of KafkaConsumers which has been shown to be problematic with multi-core executors
    // (see SPARK-19185)
    sparkConf.set("spark.streaming.kafka.consumer.cache.enabled", "false");
//      sparkConf.set("spark.sql.catalogImplementation", "in-memory");
//      sparkConf.set("spark.sql.shuffle.partitions", "1");
//      sparkConf.set("spark.sql.warehouse.dir", "target/spark-warehouse");

    if (config.hasPath(NUM_EXECUTORS_PROPERTY)) {
      sparkConf.set("spark.executor.instances", config.getString(NUM_EXECUTORS_PROPERTY));
    }

    if (config.hasPath(NUM_EXECUTOR_CORES_PROPERTY)) {
      sparkConf.set("spark.executor.cores", config.getString(NUM_EXECUTOR_CORES_PROPERTY));
    }

    if (config.hasPath(EXECUTOR_MEMORY_PROPERTY)) {
      sparkConf.set("spark.executor.memory", config.getString(EXECUTOR_MEMORY_PROPERTY));
    }

    // Override the Spark SQL shuffle partitions with the number of cores, if known.
    if (config.hasPath(NUM_EXECUTORS_PROPERTY) && config.hasPath(NUM_EXECUTOR_CORES_PROPERTY)) {
      int executors = config.getInt(NUM_EXECUTORS_PROPERTY);
      int executorCores = config.getInt(NUM_EXECUTOR_CORES_PROPERTY);
      Integer shufflePartitions = executors * executorCores;

      sparkConf.set("spark.sql.shuffle.partitions", shufflePartitions.toString());
    }

    // Allow the user to provide any Spark configuration and we will just pass it on. These can
    // also override any of the configurations above.
    if (config.hasPath(SPARK_CONF_PROPERTY_PREFIX)) {
      Config sparkConfigs = config.getConfig(SPARK_CONF_PROPERTY_PREFIX);
      for (Map.Entry<String, ConfigValue> entry : sparkConfigs.entrySet()) {
        String param = entry.getKey();
        String value = entry.getValue().unwrapped().toString();
        if (value != null) {
          sparkConf.set(param, value);
        }
      }
    }

    String mohitConf = config.root().render(ConfigRenderOptions.concise());
    sparkConf.set("mohit.configuration", mohitConf);

    return sparkConf;
  }

  public static synchronized JavaStreamingContext getJavaStreamingContext(
      SparkSession ss, Config config) {
    int batchMilliseconds = config.getInt(BATCH_MILLISECONDS_PROPERTY);
    final Duration batchDuration = Durations.milliseconds(batchMilliseconds);
    return new JavaStreamingContext(
        new JavaSparkContext(ss.sparkContext()), batchDuration);
  }

  public static synchronized String dfShowToString(Dataset<Row> df) {
    // Create a stream to hold the output
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    // IMPORTANT: Save the old System.out!
    PrintStream old = System.out;
    // Tell Java to use your special stream
    System.setOut(ps);
    // Print some output: goes to your special stream
    df.show();
    // Put things back
    System.out.flush();
    System.setOut(old);
    return baos.toString();
  }
}
