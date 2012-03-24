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

import org.testng.Assert;
import org.testng.annotations.Test;

public class WorkflowTest {

    @Test
    public void testToXMLString() throws Exception {

        final String name = getClass().getName();
        final String jobTrackerEndPoint = "localhost:54311";
        final String nameNodeEndPoint = "hdfs://localhost:54310";
        final String jobFQCN = "com.hortonworks.Foo";

        JavaAction action = new JavaAction(name, jobTrackerEndPoint, nameNodeEndPoint, jobFQCN);
        Workflow workflow = new Workflow(name, action);

        Assert.assertEquals(name, action.getName());
        Assert.assertEquals(jobFQCN, action.getClazzFQN());
        Assert.assertEquals(jobTrackerEndPoint, action.getJobTrackerURI());
        Assert.assertEquals(nameNodeEndPoint, action.getNameNodeURI());

        Assert.assertEquals(name, workflow.getName());
        Assert.assertEquals(action, workflow.getAction());
    }
}
