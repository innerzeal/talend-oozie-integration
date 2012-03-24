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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;

public class JobContextTest {

    @Test
    public void testGetJobName() throws Exception {
        JobContext jobContext = new JobContext();
        jobContext.setJobName("test");

        Assert.assertEquals("test", jobContext.getJobName());
    }

    @Test
    public void testGetJobPathOnHDFS() throws Exception {
        JobContext jobContext = new JobContext();
        jobContext.setJobPathOnHDFS("/user/foo");

        Assert.assertEquals("/user/foo", jobContext.getJobPathOnHDFS());
    }

    @Test
    public void testGetJobFQClassName() throws Exception {
        JobContext jobContext = new JobContext();
        jobContext.setJobFQClassName("com.hortonworks.foo");

        Assert.assertEquals("com.hortonworks.foo", jobContext.getJobFQClassName());
    }

    @Test
    public void testGetFrequency() throws Exception {
        JobContext jobContext = new JobContext();
        jobContext.setFrequency(15);

        Assert.assertEquals(15, jobContext.getFrequency());
    }

    @Test
    public void testGetTimeUnit() throws Exception {
        JobContext jobContext = new JobContext();
        jobContext.setTimeUnit(JobContext.Timeunit.MINUTE);

        Assert.assertEquals(JobContext.Timeunit.MINUTE, jobContext.getTimeUnit());
    }

    @Test
    public void testGetStartTime() throws Exception {
        Date start = new Date();
        JobContext jobContext = new JobContext();
        jobContext.setStartTime(start);

        Assert.assertEquals(start, jobContext.getStartTime());
    }

    @Test
    public void testGetEndTime() throws Exception {
        Date end = new Date();
        JobContext jobContext = new JobContext();
        jobContext.setEndTime(end);

        Assert.assertEquals(end, jobContext.getEndTime());
    }

    @Test
    public void testGetJobTrackerEndPoint() throws Exception {
        JobContext jobContext = new JobContext();
        jobContext.setJobTrackerEndPoint("localhost:54311");

        Assert.assertEquals("localhost:54311", jobContext.getJobTrackerEndPoint());
    }

    @Test
    public void testGetNameNodeEndPoint() throws Exception {
        JobContext jobContext = new JobContext();
        jobContext.setNameNodeEndPoint("hdfs://localhost:54310");

        Assert.assertEquals("hdfs://localhost:54310", jobContext.getNameNodeEndPoint());
    }

    @Test
    public void testGetOozieEndPoint() throws Exception {
        JobContext jobContext = new JobContext();
        jobContext.setOozieEndPoint("localhost:10000");

        Assert.assertEquals("localhost:10000", jobContext.getOozieEndPoint());
    }

    @Test
    public void testGetDebug() throws Exception {
        JobContext jobContext = new JobContext();
        jobContext.setDebug(1);

        Assert.assertEquals(1, jobContext.getDebug());
   }
}
