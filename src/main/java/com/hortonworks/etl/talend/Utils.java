/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.hortonworks.etl.talend;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Utils {

    public static final String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'";
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private Utils() {}

    public static void assertNotNull(Object value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void assertNotEmpty(String value, String message) {
        if (value == null || "".equals(value)) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static String formatDateUTC(Date d) {
        return (d != null) ? getISO8601DateFormat().format(d) : "NULL";
    }

    public static DateFormat getISO8601DateFormat() {
        DateFormat dateFormat = new SimpleDateFormat(ISO8601_DATE_FORMAT);
        dateFormat.setTimeZone(UTC);
        return dateFormat;
    }
}
