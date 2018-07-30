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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;

public class DateTimeParser {

  private List<DateTimeFormatter> formatters;

  DateTimeParser(List<String> patterns) {
    formatters = new ArrayList();
    if (patterns == null) {
      return;
    }

    for (String pattern : patterns) {
      formatters.add(DateTimeFormat.forPattern(pattern));
    }
  }

  public DateTime parse(String date) {
    if (formatters.size() == 0) {
      return DateTime.parse(date);
    }

    for (DateTimeFormatter dateTimeFormatter : formatters) {
      try {
        return dateTimeFormatter.parseDateTime(date);
      } catch (IllegalArgumentException e) {
        // ignore
      }
    }

    throw new IllegalArgumentException("Could not parse: "
        + date);
  }
}