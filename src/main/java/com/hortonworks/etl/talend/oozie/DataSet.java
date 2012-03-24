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

package com.hortonworks.etl.talend.oozie;

import com.hortonworks.etl.talend.JobContext;
import com.hortonworks.etl.talend.Utils;

import java.util.Date;

import static com.hortonworks.etl.talend.JobContext.*;

public class DataSet { // aka Feed or Table

    private final String name;
    private final int frequency;
    private final Timeunit timeUnit;
    private final String timeZone;
    private final String uriTemplate;
    private final Date initialInstance;

    public DataSet(String name, int frequency, String uriTemplate, Date initialInstance) {
        this(name, frequency, Timeunit.MINUTE, "UTC", uriTemplate, initialInstance);
    }

    public DataSet(String name, int frequency, Timeunit timeUnit, String timeZone,
                   String uriTemplate, Date initialInstance) {
        this.name = name;
        this.frequency = frequency;
        this.timeUnit = timeUnit;
        this.timeZone = timeZone;
        this.uriTemplate = uriTemplate;
        this.initialInstance = initialInstance;
    }

    public String getName() {
        return name;
    }

    public int getFrequency() {
        return frequency;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public Timeunit getTimeUnit() {
        return timeUnit;
    }

    public String getUriTemplate() {
        return uriTemplate;
    }

    public Date getInitialInstance() {
        return initialInstance;
    }

    @Override
    public String toString() {
        return "DataSet{" +
                "name='" + name + '\'' +
                ", frequency=" + frequency +
                ", timeUnit=" + timeUnit +
                ", timeZone='" + timeZone + '\'' +
                ", uriTemplate='" + uriTemplate + '\'' +
                ", initialInstance=" + initialInstance +
                '}';
    }

    public String toXMLString() {
        StringBuilder dataSetXML = new StringBuilder(8192);
        dataSetXML.append("  <datasets>");
        dataSetXML.append("    <dataset name=\"").append(name).append("\"");
        dataSetXML.append(" frequency=\"${coord:hours(").append(frequency).append(")}\"");
        dataSetXML.append(" timezone=\"").append(timeZone).append("\"");
        dataSetXML.append(" initial-instance=\"").append(Utils.formatDateUTC(initialInstance)).append("\">");
        dataSetXML.append("      <uri-template>").append(uriTemplate).append("</uri-template>");
        dataSetXML.append("    </dataset>");
        dataSetXML.append("  </datasets>");

        return dataSetXML.toString();
    }
}
