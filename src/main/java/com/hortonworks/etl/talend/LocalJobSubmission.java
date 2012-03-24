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

public class LocalJobSubmission implements JobSubmission {

    @Override
    public String submit(JobContext jobContext) throws JobSubmissionException {
        StringBuilder command = new StringBuilder(1024);
        command.append("java -cp lib/classpath.jar; ")
               .append(jobContext.getJobFQClassName())
               .append(" --context=Default %*");

        try {
            Process process = Runtime.getRuntime().exec(command.toString());
            int returnValue = process.waitFor();
            return jobContext.getJobName() + (returnValue == 0 ? "-Completed" : "-Failed");
        }
        catch (Exception e) {
            throw new JobSubmissionException("Error executing the etl job.", e);
        }
    }

    @Override
    public String resubmit(String jobHandle, JobContext jobContext) throws JobSubmissionException {
        throw new UnsupportedOperationException("This operation does not make sense for local/sync execution!");
    }

    @Override
    public Status status(String jobHandle, String oozieEndPoint) throws JobSubmissionException {
        throw new UnsupportedOperationException("This operation does not make sense for local/sync execution!");
    }

    @Override
    public void kill(String jobHandle, String oozieEndPoint) throws JobSubmissionException {
        throw new UnsupportedOperationException("This operation does not make sense for local/sync execution!");
    }

    @Override
    public String getJobLog(String jobHandle, String oozieEndPoint) throws JobSubmissionException {
        throw new UnsupportedOperationException("This operation does not make sense for local/sync execution!");
    }
}
