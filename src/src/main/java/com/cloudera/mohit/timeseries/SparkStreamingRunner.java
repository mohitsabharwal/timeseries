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
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

public class SparkStreamingRunner {
  private static Logger LOG = LoggerFactory.getLogger(SparkStreamingRunner.class);
  private static final String GROUP_ID_CONFIG = "kafka.group.id";
  private static final String OUTPUT_FILE = "output.file";
  private static final String DELIMITER_CONFIG_NAME = "input.delimiter";
  private static final String FIELD_NAMES_CONFIG_NAME = "input.field.names";
  private static final String FIELD_TYPES_CONFIG_NAME = "input.field.types";
  private static final String TIMESTAMP_FORMAT_CONFIG_NAME = "timestamp.format";


  @SuppressWarnings({ "serial", "rawtypes" })
  private static class UnwrapConsumerRecordFunction implements PairFunction<Object, String, String>, Serializable {
    @Override
    public Tuple2<String, String> call(Object recordObject) throws Exception {
      ConsumerRecord record = (ConsumerRecord)recordObject;
      return new Tuple2("foo", record.value().toString());
    }
  }

  @SuppressWarnings("serial")
  private static class TranslateFunction implements FlatMapFunction<Tuple2<String, String>, Row> {
    private List<String> fieldTypes;
    private List<Object> values = Lists.newArrayList();
    private String delimiter;
    private List<String> fieldNames;
    private List<String> timestampFormats;

    TranslateFunction(List<String> fieldNames,List<String> fieldTypes, String delimiter,
                      List<String> timestampFormats) {
      this.fieldNames = fieldNames;
      this.fieldTypes = fieldTypes;
      this.delimiter = delimiter;
      this.timestampFormats = timestampFormats;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> call(Tuple2<String, String> keyAndValue) throws Exception {
      String value = keyAndValue._2.toString();
      int numFields = fieldNames.size();
      String[] stringValues = value.split(Pattern.quote(delimiter), fieldNames.size());
      values.clear();

      for (int valuePos = 0; valuePos < numFields; valuePos++) {
        if (valuePos < stringValues.length) {

          try {
            String fieldValue = stringValues[valuePos];
            if (fieldValue.length() == 0) {
              values.add(null);
            } else {
              switch (fieldTypes.get(valuePos)) {
                case "string":
                  values.add(fieldValue);
                  break;
                case "float":
                  values.add(Float.parseFloat(fieldValue));
                  break;
                case "double":
                  values.add(Double.parseDouble(fieldValue));
                  break;
                case "int":
                  values.add(Integer.parseInt(fieldValue));
                  break;
                case "long":
                  values.add(Long.parseLong(fieldValue));
                  break;
                case "boolean":
                  values.add(Boolean.parseBoolean(fieldValue));
                  break;
                case "timestamp":
                  DateTimeParser parser = new DateTimeParser(timestampFormats);
                  values.add(new Timestamp(parser.parse(fieldValue).getMillis()));
                  break;
                default:
                  throw new RuntimeException("Unsupported delimited field type: " +
                      fieldTypes.get(valuePos));
              }
            }
          } catch(Exception ex) {
            LOG.error("Input field type mismatch. Adding null for field " +
                fieldNames.get(valuePos) + " which is of type " +
                fieldTypes.get(valuePos) +  " .This field could not be " +
                " converted to given type in input [" + value + "]. Exception: " +
                "following exception " + ex.toString());
            ex.printStackTrace();
            values.add(null);
          }

        } else {
          values.add(null);
        }
      }

      Row row = RowFactory.create(values.toArray());
      return Collections.singleton(row).iterator();
    }
  }

  public static void runStreaming(final Config config) throws Exception {
    final String groupID;
    if (config.hasPath(GROUP_ID_CONFIG)) {
      groupID = config.getString(GROUP_ID_CONFIG);
    } else {
      groupID = UUID.randomUUID().toString();
    }

    final String outputFile;
    if (config.hasPath(OUTPUT_FILE)) {
      outputFile = config.getString(OUTPUT_FILE);
    } else {
      outputFile = null;
    }

    final String delimiter = config.getString(DELIMITER_CONFIG_NAME);
    final List<String> fieldNames = config.getStringList(FIELD_NAMES_CONFIG_NAME);
    final List<String> fieldTypes = config.getStringList(FIELD_TYPES_CONFIG_NAME);
    final List<String> timestampFormats;
    if (config.hasPath(TIMESTAMP_FORMAT_CONFIG_NAME)) {
      timestampFormats = config.getStringList(TIMESTAMP_FORMAT_CONFIG_NAME);
    } else {
      timestampFormats = null;
    }

    final StructType schema = RowUtils.structTypeFor(fieldNames, fieldTypes);

    final SparkSession ss = SparkUtils.getSparkSession(config);
    JavaStreamingContext jsc = SparkUtils.getJavaStreamingContext(ss, config);
    JavaDStream stream = KafkaUtils.getDStream(jsc, groupID, config);
    stream.foreachRDD(new VoidFunction<JavaRDD<Object>>() {
      @Override
      public void call(JavaRDD<Object> rdd) throws Exception {
        if (rdd.isEmpty()) {
          return;
        }

        JavaPairRDD<String, String> prepared= rdd.mapToPair(
            new UnwrapConsumerRecordFunction());
        JavaRDD<Row> translated = prepared.flatMap(
            new TranslateFunction(fieldNames, fieldTypes, delimiter, timestampFormats));
        Dataset<Row> df = ss.createDataFrame(translated, schema);
        LOG.debug(SparkUtils.dfShowToString(df));
        if (outputFile != null) {
          FilesystemOutput.write(df, outputFile);
        }

        OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        KafkaUtils.upsertOffsets(groupID, offsetRanges);
      }
    });

    jsc.start();
    LOG.debug("Streaming context started");
    jsc.awaitTermination();
    LOG.debug("Streaming context terminated");
  }
}