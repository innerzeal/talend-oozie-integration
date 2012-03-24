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

import com.hortonworks.etl.talend.oozie.RemoteJobSubmission;
import com.hortonworks.etl.talend.oozie.ScheduledJobSubmission;

/**
 * Factory class to dole out JobSubmission implementations.
 *      - If frequency and timeunit are specified, its a scheduled, recurring job submission
 *      - If frequency and timeunit are not specified but a OozieEndPoint is, its a remote job submission
 *      - Else Local Job
 */
public class JobSubmissionFactory {

    private JobSubmissionFactory() {
    }

    public static JobSubmission get(JobContext jobContext) {
        if (scheduled(jobContext)) {
            return new ScheduledJobSubmission();
        }
        else if (remote(jobContext)) {
            return new RemoteJobSubmission();
        }
        else {
            return new LocalJobSubmission();
        }
    }

    private static boolean scheduled(JobContext jobContext) {
        return jobContext.getFrequency() > 0 && jobContext.getTimeUnit() != JobContext.Timeunit.NONE;
    }

    private static boolean remote(JobContext jobContext) {
        return jobContext.getOozieEndPoint() != null;
    }
}
