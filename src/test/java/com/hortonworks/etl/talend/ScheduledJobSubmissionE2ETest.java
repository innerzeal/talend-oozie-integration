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

import com.hortonworks.etl.talend.oozie.ScheduledJobSubmission;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;

import java.util.Calendar;
import java.util.Date;

public class ScheduledJobSubmissionE2ETest {

    public static void main(String[] args) throws Exception {
        final String name = "ScheduledJobSubmissionE2ETest";
        final String path = "/user/seetharam/etl/talend/unit_test/scheduled";
        final String jobTrackerEndPoint = "localhost:54311";
        final String nameNodeEndPoint = "hdfs://localhost:54310";
        final String oozieEndPoint = "http://localhost:11000/oozie";
        final String jobFQCN = "talenddemosjava.mysql_to_hdfs_0_1.mysql_to_hdfs";

        JobContext jobContext = new JobContext();
        jobContext.setJobName(name);
        jobContext.setJobFQClassName(jobFQCN);
        jobContext.setJobPathOnHDFS(path);

        Calendar instance = Calendar.getInstance();
        Date start = instance.getTime();
        jobContext.setStartTime(start);
        instance.roll(3, 1);
        Date end = instance.getTime();
        jobContext.setEndTime(end);

        jobContext.setFrequency(1);
        jobContext.setTimeUnit(JobContext.Timeunit.MINUTE);

        jobContext.setJobTrackerEndPoint(jobTrackerEndPoint);
        jobContext.setNameNodeEndPoint(nameNodeEndPoint);
        jobContext.setOozieEndPoint(oozieEndPoint);

        jobContext.setTosContextPath("Default");

        ScheduledJobSubmission scheduledJobSubmission = new ScheduledJobSubmission();
        final String scheduledJobHandle = scheduledJobSubmission.submit(jobContext);
        Assert.assertTrue(scheduledJobHandle != null);
        System.out.println("scheduledJobHandle = " + scheduledJobHandle);

        OozieClient oozieClient = new OozieClient(oozieEndPoint);
        CoordinatorJob coordinatorJob = oozieClient.getCoordJobInfo(scheduledJobHandle);
        Assert.assertTrue(coordinatorJob != null);
        Assert.assertEquals(coordinatorJob.getId(), scheduledJobHandle);

        JobSubmission.Status status = scheduledJobSubmission.status(scheduledJobHandle, jobContext.getOozieEndPoint());
        Assert.assertTrue(status != null);
//        Assert.assertTrue(status != JobSubmission.Status.RUNNING);
        Assert.assertEquals(status, JobSubmission.Status.PREP);  // since exec is sync

        System.out.println("After Launch: " + oozieClient.getJobInfo(scheduledJobHandle));

        Thread.sleep(1000 * 60 * 5);

        scheduledJobSubmission.kill(scheduledJobHandle, jobContext.getOozieEndPoint());
        System.out.println("After Kill: " + oozieClient.getJobInfo(scheduledJobHandle));
        status = scheduledJobSubmission.status(scheduledJobHandle, jobContext.getOozieEndPoint());
        Assert.assertTrue(status != null);
        Assert.assertEquals(status, JobSubmission.Status.KILLED);

        String scheduledJobLog = scheduledJobSubmission.getJobLog(scheduledJobHandle, jobContext.getOozieEndPoint());
        Assert.assertTrue(scheduledJobLog != null);
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("scheduledJobLog = " + scheduledJobLog);
    }
}
