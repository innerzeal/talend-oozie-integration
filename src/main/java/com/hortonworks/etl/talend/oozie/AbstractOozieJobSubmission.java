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
import com.hortonworks.etl.talend.JobSubmission;
import com.hortonworks.etl.talend.JobSubmissionException;
import com.hortonworks.etl.talend.Utils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

import java.io.IOException;

public abstract class AbstractOozieJobSubmission implements JobSubmission {

    @Override
    public String resubmit(String jobHandle, JobContext jobContext) throws JobSubmissionException {
        kill(jobHandle, jobContext.getOozieEndPoint());
        return submit(jobContext);
    }

    @Override
    public void kill(String jobHandle, String oozieEndPoint) throws JobSubmissionException {
        OozieClient oozieClient = createOozieClient(oozieEndPoint, 0);
        try {
            oozieClient.kill(jobHandle);
        } catch (OozieClientException e) {
            throw new JobSubmissionException("Error killing job: " + jobHandle, e);
        }
    }

    @Override
    public String getJobLog(String jobHandle, String oozieEndPoint) throws JobSubmissionException {
        try {
            return createOozieClient(oozieEndPoint, 0).getJobLog(jobHandle);
        } catch (OozieClientException e) {
            throw new JobSubmissionException("Error fetching job job for: " + jobHandle, e);
        }
    }

    protected OozieClient createOozieClient(String oozieEndPoint, int debug) {
        OozieClient oozieClient = new OozieClient(oozieEndPoint);
        oozieClient.setDebugMode(debug);

        return oozieClient;
    }

    protected void createWorkflowTemplate(JobContext jobContext) throws IOException {
        Workflow workflow = createWorkflow(jobContext);
        serializeToHDFS(workflow.toXMLString(), "/workflow.xml", jobContext);
    }

    protected Workflow createWorkflow(JobContext jobContext) {
        JavaAction action = new JavaAction(jobContext.getJobName(), jobContext.getJobTrackerEndPoint(),
                                           jobContext.getNameNodeEndPoint(), jobContext.getJobFQClassName());
        String tosContextPath = jobContext.getTosContextPath();
        if (tosContextPath != null) {
            action.addArgument("--context=" + tosContextPath);
        }
        return new Workflow(jobContext.getJobName(), action);
    }

    protected void createCoordinatorTemplate(JobContext jobContext) throws IOException {
        Coordinator coordinator = createCoordinator(jobContext);
        serializeToHDFS(coordinator.toXMLString(), "/coordinator.xml", jobContext);
    }

    protected Coordinator createCoordinator(JobContext jobContext) {
        Utils.assertTrue(jobContext.getFrequency() > 0, "Frequency has to be greater than 0.");

        return new Coordinator(jobContext.getJobName(),
                               jobContext.getNameNodeEndPoint() + jobContext.getJobPathOnHDFS(),
                               jobContext.getStartTime(), jobContext.getEndTime(),
                               jobContext.getFrequency(), jobContext.getTimeUnit());
    }

    protected void serializeToHDFS(String toSerialize, String asFile, JobContext jobContext) throws IOException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.default.name", jobContext.getNameNodeEndPoint());
        FileSystem fs = FileSystem.get(configuration);

        Path wfFile = new Path(jobContext.getNameNodeEndPoint() + jobContext.getJobPathOnHDFS() + asFile);
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(wfFile);
            outputStream.writeBytes(toSerialize);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }
}
