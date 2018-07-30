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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaUtils {
  private static Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

  public static final String BROKERS_CONFIG = "kafka.brokers";
  public static final String TOPIC_CONFIG = "kafka.topics";
  public static final String ENCODING_CONFIG = "kafka.encoding";
  public static final String WINDOW_ENABLED_CONFIG = "kafka.window.enabled";
  public static final String WINDOW_MILLISECONDS_CONFIG = "kafka.window.milliseconds";
  public static final String WINDOW_SLIDE_MILLISECONDS_CONFIG = "kafka.window.slide.milliseconds";
  public static final String PARAMETER_CONFIG_PREFIX = "parameter.";

  public static void upsertOffsets(String groupID, OffsetRange[] offsetRanges) throws Exception {
    // Plan the offset ranges as an upsert
    List<Row> planned = Lists.newArrayList();
    StructType schema = DataTypes.createStructType(Lists.newArrayList(
        DataTypes.createStructField("group_id", DataTypes.StringType, false),
        DataTypes.createStructField("topic", DataTypes.StringType, false),
        DataTypes.createStructField("partition", DataTypes.IntegerType, false),
        DataTypes.createStructField("offset", DataTypes.LongType, false)));
    for (OffsetRange offsetRange : offsetRanges) {
      Row offsetRow = new RowWithSchema(schema, groupID, offsetRange.topic(),
          offsetRange.partition(), offsetRange.untilOffset());
      planned.add(offsetRow);
    }

    // Upsert the offset ranges at the output
    ZookeeperStore.upsert(planned);

    // Retrieve back the offset ranges and assert that they were stored correctly
    for (OffsetRange offsetRange : offsetRanges) {
      Map<TopicPartition, Long> storedOffsets = getLastOffsets(Lists.newArrayList(offsetRange.topic()),
          groupID);
      TopicPartition tp = new TopicPartition(offsetRange.topic(), offsetRange.partition());
      if (!storedOffsets.containsKey(tp)) {
        throw new RuntimeException("Topic " + offsetRange.topic() + " and Partition " + offsetRange.partition()
            + " was not stored!");
      }

      if (!storedOffsets.get(tp).equals(offsetRange.untilOffset())) {
        String exceptionMessage = String.format(
            "Kafka input failed to assert that offset ranges were stored correctly! " +
                "For group ID '%s', topic '%s', partition '%d' expected offset '%d' but found offset '%d'",
            groupID, offsetRange.topic(), tp.partition(), offsetRange.untilOffset(), storedOffsets.get(tp));
        throw new RuntimeException(exceptionMessage);
      }
    }
  }


  private static Map<TopicPartition, Long> getLastOffsets(List<String> topics, String groupID)
      throws Exception {
    Map<TopicPartition, Long> offsetRanges = Maps.newHashMap();
    // Create filter for groupid/topic
    for (String topic : topics) {
      StructType filterSchema = DataTypes.createStructType(Lists.newArrayList(
          DataTypes.createStructField("group_id", DataTypes.StringType, false),
          DataTypes.createStructField("topic", DataTypes.StringType, false)));
      Row groupIDTopicFilter = new RowWithSchema(filterSchema, groupID, topic);
      Iterable<Row> filters = Collections.singleton(groupIDTopicFilter);

      // Get results
      Iterable<Row> results = ZookeeperStore.get(filters);

      // Transform results into map
      for (Row result : results) {
        Integer partition = result.getInt(result.fieldIndex("partition"));
        Long offset = result.getLong(result.fieldIndex("offset"));
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        offsetRanges.put(topicPartition, offset);
      }
    }

    return offsetRanges;
  }

  /**
     * Add custom parameters to the configuration key-value map
     * @param params map to which to add new parameters
     * @param config config for the input/output
     */
  private static void addCustomParams(Map<String, Object> params, Config config) {
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      String propertyName = entry.getKey();
      if (propertyName.startsWith(PARAMETER_CONFIG_PREFIX)) {
        String paramName = propertyName.substring(PARAMETER_CONFIG_PREFIX.length());
        String paramValue = config.getString(propertyName);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding Kafka property: {} = \"{}\"", paramName, paramValue);
        }
        params.put(paramName, paramValue);
      }
    }
  }

  public static JavaDStream<?> getDStream(JavaStreamingContext jssc,
                                          String groupID,
                                          Config config) throws Exception {
    Map<String, Object> kafkaParams = Maps.newHashMap();
    String brokers = config.getString(BROKERS_CONFIG);
    kafkaParams.put("bootstrap.servers", brokers);
    List<String> topics = config.getStringList(TOPIC_CONFIG);
    Set<String> topicSet = Sets.newHashSet(topics);
    String encoding = config.getString(ENCODING_CONFIG);
    if (encoding.equals("string")) {
      kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }
    else if (encoding.equals("bytearray")) {
      kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }
    else {
      throw new RuntimeException("Invalid Kafka input encoding type. Valid types are 'string' and 'bytearray'.");
    }

    kafkaParams.put("group.id", groupID);
    kafkaParams.put("enable.auto.commit", "false");
    addCustomParams(kafkaParams, config);
    JavaDStream<?> dStream;
    if (!encoding.equals("string")) {
      throw new RuntimeException("Invalid Kafka input encoding type.");
    }

    Map<TopicPartition, Long> partitionOffsets = getLastOffsets(topics, groupID);
    if (!partitionOffsets.isEmpty()) {
      dStream = org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
          ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams, partitionOffsets));
    } else {
      LOG.warn("partitionOffsets are empty when creating direct stream");
      dStream = org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
          ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams));
    }

    if (config.hasPath(WINDOW_ENABLED_CONFIG) && config.getBoolean(WINDOW_ENABLED_CONFIG)) {
      int windowDuration = config.getInt(WINDOW_MILLISECONDS_CONFIG);

      if (config.hasPath(WINDOW_SLIDE_MILLISECONDS_CONFIG)) {
        int slideDuration = config.getInt(WINDOW_SLIDE_MILLISECONDS_CONFIG);
        dStream = dStream.window(new Duration(windowDuration), new Duration(slideDuration));
      } else {
        dStream = dStream.window(new Duration(windowDuration));
      }
    }

    return dStream;
  }

}
