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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Calendar;
import java.util.Date;

public class CoordinatorTest {

    @Test
    public void testToXMLString() throws Exception {
        Calendar instance = Calendar.getInstance();
        Date start = instance.getTime();
        instance.roll(3, 1);
        Date end = instance.getTime();

        final String name = getClass().getName();
        final String path = "hdfs://localhost:54310/user/foo/etl/ooze/job";


        Coordinator coordinator = new Coordinator(name, path, start, end, 1, JobContext.Timeunit.MINUTE);
        System.out.println("c.toXMLString() = " + coordinator.toXMLString());

        Assert.assertEquals(start, coordinator.getStartTime());
        Assert.assertEquals(end, coordinator.getEndTime());
        Assert.assertEquals(name, coordinator.getName());
        Assert.assertEquals(path, coordinator.getPath());
    }

    @Test
    public void testGetCoordinatorFrequency() throws Exception {
        Calendar instance = Calendar.getInstance();
        Date start = instance.getTime();
        instance.roll(3, 1);
        Date end = instance.getTime();

        final String name = getClass().getName();
        final String path = "hdfs://localhost:54310/user/foo/etl/ooze/job";

        Coordinator coordinator = new Coordinator(name, path, start, end, 1, JobContext.Timeunit.MINUTE);

        String coordinatorFrequency = coordinator.getCoordinatorFrequency(60, JobContext.Timeunit.MINUTE);
        Assert.assertEquals(" frequency=\"${coord:minutes(60)}\"", coordinatorFrequency);

        coordinatorFrequency = coordinator.getCoordinatorFrequency(2, JobContext.Timeunit.HOUR);
        Assert.assertEquals(" frequency=\"${coord:hours(2)}\"", coordinatorFrequency);

        coordinatorFrequency = coordinator.getCoordinatorFrequency(7, JobContext.Timeunit.DAY);
        Assert.assertEquals(" frequency=\"${coord:days(7)}\"", coordinatorFrequency);

        coordinatorFrequency = coordinator.getCoordinatorFrequency(2, JobContext.Timeunit.WEEK);
        Assert.assertEquals(" frequency=\"${coord:weeks(2)}\"", coordinatorFrequency);

        coordinatorFrequency = coordinator.getCoordinatorFrequency(3, JobContext.Timeunit.MONTH);
        Assert.assertEquals(" frequency=\"${coord:months(3)}\"", coordinatorFrequency);
    }
}
