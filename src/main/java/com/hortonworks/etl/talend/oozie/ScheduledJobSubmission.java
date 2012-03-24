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

import java.io.IOException;
import java.util.Properties;

/**
 * JobSubmission implementation that schedules the recurring execution of
 * the ETL job on the Hadoop Cluster using Oozie Coordinator.
 */
public class ScheduledJobSubmission extends AbstractOozieJobSubmission {

    @Override
    public String submit(JobContext jobContext) throws JobSubmissionException {
        try {
            createWorkflowTemplate(jobContext);
            createCoordinatorTemplate(jobContext);

            return doSubmit(jobContext);
        } catch (IOException e) {
            throw new JobSubmissionException("Error serializing coordinator xml to HDFS.", e);
        } catch (OozieClientException e) {
            throw new JobSubmissionException("Error submitting coordinator job to Oozie.", e);
        }
    }

    protected String doSubmit(JobContext jobContext) throws OozieClientException {
        OozieClient oozieClient = createOozieClient(jobContext.getOozieEndPoint(),
                                                    jobContext.getDebug());

        // create a coordinator job configuration and set the coordinator application path
        Properties configuration = oozieClient.createConfiguration();
        configuration.setProperty(OozieClient.COORDINATOR_APP_PATH, jobContext.getNameNodeEndPoint() + jobContext.getJobPathOnHDFS());

        // start the coordinator job
        return oozieClient.run(configuration);
    }

    @Override
    public Status status(String jobHandle, String oozieEndPoint) throws JobSubmissionException {
        try {
            OozieClient oozieClient = createOozieClient(oozieEndPoint, 0);
            String status = oozieClient.getCoordJobInfo(jobHandle).getStatus().name();
            return Status.valueOf(status);
        } catch (OozieClientException e) {
            throw new JobSubmissionException("Error getting status for job: " + jobHandle, e);
        }
    }
}
