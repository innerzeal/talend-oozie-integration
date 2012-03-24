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

/**
 * JobSubmission interface to launch the execution of an ETL job
 */
public interface JobSubmission {

    public static enum Status {
        PREP, RUNNING, SUCCEEDED, KILLED, FAILED, SUSPENDED
    }

    /**
     * Submit an ETL job for execution
     *
     * @param jobContext - job context parameters
     * @return external job id or handle
     * @throws JobSubmissionException wrapper for exceptions from HDFS and Oozie
     */
    String submit(JobContext jobContext) throws JobSubmissionException;

    /**
     * Redeploy an ETL job that is already deployed on Oozie
     *
     * @param jobHandle external job handle that was returned as part of the initial job submission
     * @param jobContext job context parameters
     * @return external job id or handle
     * @throws JobSubmissionException wrapper for exceptions from HDFS and Oozie
     */
    String resubmit(String jobHandle, JobContext jobContext) throws JobSubmissionException;

    /**
     * Determine the current status of a previously submitted job
     *
     * @param jobHandle external job handle that was returned as part of the initial job submission
     * @param oozieEndPoint Web Service End Point of Oozie Scheduler Service
     * @return Status Current status of the job submitted
     * @throws JobSubmissionException wrapper for exceptions from HDFS and Oozie
     */
    Status status(String jobHandle, String oozieEndPoint) throws JobSubmissionException;

    /**
     * Kill a previously submitted job
     *
     * @param jobHandle external job handle that was returned as part of the initial job submission
     * @param oozieEndPoint Web Service End Point of Oozie Scheduler Service
     * @throws JobSubmissionException wrapper for exceptions from HDFS and Oozie
     */
    void kill(String jobHandle, String oozieEndPoint) throws JobSubmissionException;

    // JobInfo info(String jobHandle) throws JobSubmissionException;

    /**
     * Retrieve the logs of a previously submitted job
     *
     * @param jobHandle external job handle that was returned as part of the initial job submission
     * @param oozieEndPoint Web Service End Point of Oozie Scheduler Service
     * @return Log contents as a String
     * @throws JobSubmissionException wrapper for exceptions from HDFS and Oozie
     */
    String getJobLog(String jobHandle, String oozieEndPoint) throws JobSubmissionException;
}
