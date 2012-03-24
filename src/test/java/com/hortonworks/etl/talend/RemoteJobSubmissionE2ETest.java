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
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;

public class RemoteJobSubmissionE2ETest {

    public static void main(String[] args) throws Exception {
        final String name = "RemoteJobSubmissionE2ETest";
        final String path = "/user/seetharam/etl/talend/unit_test/remote";
        final String jobTrackerEndPoint = "localhost:54311";
        final String nameNodeEndPoint = "hdfs://localhost:54310";
        final String oozieEndPoint = "http://localhost:11000/oozie";
        final String jobFQCN = "talenddemosjava.mysql_to_hdfs_0_1.mysql_to_hdfs";

        JobContext jobContext = new JobContext();
        jobContext.setJobName(name);
        jobContext.setJobTrackerEndPoint(jobTrackerEndPoint);
        jobContext.setNameNodeEndPoint(nameNodeEndPoint);
        jobContext.setJobFQClassName(jobFQCN);
        jobContext.setJobPathOnHDFS(path);
        jobContext.setOozieEndPoint(oozieEndPoint);

        RemoteJobSubmission remoteJobSubmission = new RemoteJobSubmission();
        final String remoteJobHandle = remoteJobSubmission.submit(jobContext);
        Assert.assertTrue(remoteJobHandle != null);
        System.out.println("remoteJobHandle = " + remoteJobHandle);

        OozieClient oozieClient = new OozieClient(oozieEndPoint);
        WorkflowJob workflowJob = oozieClient.getJobInfo(remoteJobHandle);
        Assert.assertTrue(workflowJob != null);
        Assert.assertEquals(workflowJob.getId(), remoteJobHandle);

        JobSubmission.Status status = remoteJobSubmission.status(remoteJobHandle, jobContext.getOozieEndPoint());
        Assert.assertTrue(status != null);
        Assert.assertTrue(status != JobSubmission.Status.RUNNING);  // since exec is sync
        Assert.assertEquals(status, JobSubmission.Status.SUCCEEDED);  // since exec is sync

//        System.out.println("Workflow Job: " + remoteJobHandle + ", Status: " + status);
        System.out.println("After Launch: " + oozieClient.getJobInfo(remoteJobHandle));

        remoteJobSubmission.kill(remoteJobHandle, jobContext.getOozieEndPoint());
        System.out.println("After Kill: " + oozieClient.getJobInfo(remoteJobHandle));
        status = remoteJobSubmission.status(remoteJobHandle, jobContext.getOozieEndPoint());
        Assert.assertTrue(status != null);
        Assert.assertEquals(status, JobSubmission.Status.KILLED);

        String remoteJobLog = remoteJobSubmission.getJobLog(remoteJobHandle, jobContext.getOozieEndPoint());
        Assert.assertTrue(remoteJobLog != null);
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("remoteJobLog = " + remoteJobLog);
    }
}
