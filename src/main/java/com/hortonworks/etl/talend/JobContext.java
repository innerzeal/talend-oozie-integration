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

import org.apache.hadoop.conf.Configuration;

import java.util.Date;

public class JobContext {

    public static final String NAME_NODE_END_POINT = "nameNodeEndPoint";
    public static final String JOB_TRACKER_END_POINT = "jobTrackerEndPoint";
    public static final String OOZIE_END_POINT = "oozieEndPoint";
    public static final String JOB_PATH_ON_HDFS = "jobPathOnHDFS";
    public static final String TIME_UNIT = "timeunit";
    public static final String FREQUENCY = "frequency";
    public static final String JOB_FQ_CLASS_NAME = "jobFQClassName";
    public static final String JOB_NAME = "jobName";
    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";
    public static final String TOS_CONTEXT_PATH = "tosContext";

    /**
     * Defines the possible frequency unit of an Oozie application.
     */
    public static enum Timeunit {
        MINUTE, HOUR, DAY, WEEK, MONTH, END_OF_DAY, END_OF_MONTH, NONE
    }

    private final Configuration configuration;

    public JobContext() {
        this(new Configuration());
    }

    public JobContext(Configuration configuration) {
        this.configuration = configuration;
    }

    public void set(String name, String value) {
        configuration.set(name, value);
    }

    public String get(String name) {
        return configuration.get(name);
    }

    public String getJobName() {
        return configuration.get(JOB_NAME);
    }

    public void setJobName(String jobName) {
        configuration.set(JOB_NAME, jobName);
    }

    /**
     * Does not include the NameNode scheme://host:port
     *
     * @return job path on hdfs that does not include the NameNode scheme://host:port
     */
    public String getJobPathOnHDFS() {
        return configuration.get(JOB_PATH_ON_HDFS);
    }

    /**
     * Plain path that should not include the NameNode scheme://host:port
     *
     * @param jobPathOnHDFS
     */
    public void setJobPathOnHDFS(String jobPathOnHDFS) {
        configuration.set(JOB_PATH_ON_HDFS, jobPathOnHDFS);
    }

    /**
     * Return the fully qualified Class Name for the ETL Job
     *
     * @return TOS ETL Job's Fully Qualified Class Name
     */
    public String getJobFQClassName() {
        return configuration.get(JOB_FQ_CLASS_NAME);
    }

    /**
     * Set the fully qualified Class Name for the ETL Job
     *
     * @param jobFQClassName TOS ETL Job's Fully Qualified Class Name
     */
    public void setJobFQClassName(String jobFQClassName) {
        configuration.set(JOB_FQ_CLASS_NAME, jobFQClassName);
    }

    public int getFrequency() {
        return configuration.getInt(FREQUENCY, 0);
    }

    public void setFrequency(int frequency) {
        configuration.setInt(FREQUENCY, frequency);
    }

    public Timeunit getTimeUnit() {
        return configuration.getEnum(TIME_UNIT, Timeunit.NONE);
    }

    public void setTimeUnit(Timeunit timeunit) {
        configuration.setEnum(TIME_UNIT, timeunit);
    }

    public Date getStartTime() {
        long startTime = configuration.getLong(START_TIME, 0L);
        return (startTime != 0) ? new Date(startTime) : null;
    }

    public void setStartTime(Date startTime) {
        configuration.setLong(START_TIME, startTime.getTime());
    }

    public Date getEndTime() {
        long endTime = configuration.getLong(END_TIME, 0L);
        return (endTime != 0) ? new Date(endTime) : null;
    }

    public void setEndTime(Date endTime) {
        configuration.setLong(END_TIME, endTime.getTime());
    }

    public String getJobTrackerEndPoint() {
        return configuration.get(JOB_TRACKER_END_POINT);
    }

    public void setJobTrackerEndPoint(String jobTrackerEndPoint) {
        configuration.set(JOB_TRACKER_END_POINT, jobTrackerEndPoint);
    }

    public String getNameNodeEndPoint() {
        return configuration.get(NAME_NODE_END_POINT);
    }

    public void setNameNodeEndPoint(String nameNodeEndPoint) {
        configuration.set(NAME_NODE_END_POINT, nameNodeEndPoint);
    }

    public String getOozieEndPoint() {
        return configuration.get(OOZIE_END_POINT);
    }

    public void setOozieEndPoint(String oozieEndPoint) {
        configuration.set(OOZIE_END_POINT, oozieEndPoint);
    }
    
    public int getDebug() {
        return configuration.getInt("debug", 0);
    }

    public void setDebug(int debug) {
        configuration.setInt("debug", 1);
    }
    
    public String getTosContextPath() {
        return configuration.get(TOS_CONTEXT_PATH);
    }

    public void setTosContextPath(String tosContextPath) {
        configuration.set(TOS_CONTEXT_PATH, tosContextPath);
    }
}
