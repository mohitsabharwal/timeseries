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
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Timeseries {
  private static Logger LOG = LoggerFactory.getLogger(Timeseries.class);

  public static void main( String[] args ) throws Exception {
    LOG.info("Starting Timeseries application...");
    if (args.length != 1) {
      LOG.error("Configuration file missing");
    }

    Config config = ConfigFactory.parseFile(new File(args[0]));
    LOG.info(config.root().render());
    ZookeeperStore.configure(config);
    SparkStreamingRunner.runStreaming(config);
  }
}