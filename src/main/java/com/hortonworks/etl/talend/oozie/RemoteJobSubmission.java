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
import com.hortonworks.etl.talend.JobSubmissionException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.io.IOException;
import java.util.Properties;

/**
 * JobSubmission implementation that launches the ETL job on the Hadoop Cluster using Oozie workflow
 */
public class RemoteJobSubmission extends AbstractOozieJobSubmission {

    @Override
    public String submit(JobContext jobContext) throws JobSubmissionException {
        try {
            createWorkflowTemplate(jobContext);

            return doSubmit(jobContext);
        } catch (IOException e) {
            throw new JobSubmissionException("Error serializing workflow xml to HDFS.", e);
        } catch (OozieClientException e) {
            throw new JobSubmissionException("Error submitting workflow job to Oozie.", e);
        }
    }

    protected String doSubmit(JobContext jobContext) throws OozieClientException {
        OozieClient oozieClient = createOozieClient(jobContext.getOozieEndPoint(),
                                                    jobContext.getDebug());

        // create a workflow job configuration and set the workflow application path
        Properties configuration = oozieClient.createConfiguration();
        configuration.setProperty(OozieClient.APP_PATH, jobContext.getNameNodeEndPoint() + jobContext.getJobPathOnHDFS());

        // start the workflow job
        String jobHandle = oozieClient.run(configuration);
        waitForJobCompletion(jobHandle, oozieClient);

        return jobHandle;
    }

    protected void waitForJobCompletion(String jobHandle, OozieClient oozieClient) throws OozieClientException {
        // wait until the workflow job finishes, printing the status every 10 seconds
        while (oozieClient.getJobInfo(jobHandle).getStatus() == WorkflowJob.Status.RUNNING) {
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException ignore) {
            }
        }
    }

    @Override
    public Status status(String jobHandle, String oozieEndPoint) throws JobSubmissionException {
        try {
            OozieClient oozieClient = createOozieClient(oozieEndPoint, 0);
            String status = oozieClient.getJobInfo(jobHandle).getStatus().name();
            return Status.valueOf(status);
        } catch (OozieClientException e) {
            throw new JobSubmissionException("Error getting status for job: " + jobHandle, e);
        }
    }
}
