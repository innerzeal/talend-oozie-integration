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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaAction {
    private final String name;
    private final String jobTrackerURI;
    private final String nameNodeURI;
    private final String clazzFQN;
    private boolean captureOutput;
    private List<String> arguments;

    public JavaAction(String name, String jobTrackerURI, String nameNodeURI, String clazzFQN) {
        this.name = name;
        this.jobTrackerURI = jobTrackerURI;
        this.nameNodeURI = nameNodeURI;
        this.clazzFQN = clazzFQN;
        arguments = new ArrayList<String>();
    }

    public String getName() {
        return name;
    }

    public String getJobTrackerURI() {
        return jobTrackerURI;
    }

    public String getNameNodeURI() {
        return nameNodeURI;
    }

    public String getClazzFQN() {
        return clazzFQN;
    }

    public boolean isCaptureOutput() {
        return captureOutput;
    }

    public void setCaptureOutput(boolean captureOutput) {
        this.captureOutput = captureOutput;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public void addArgument(String argument) {
        this.arguments.add(argument);
    }

    @Override
    public String toString() {
        return "JavaAction{" +
                "name='" + name + '\'' +
                ", jobTrackerURI='" + jobTrackerURI + '\'' +
                ", nameNodeURI='" + nameNodeURI + '\'' +
                ", clazzFQN='" + clazzFQN + '\'' +
                ", captureOutput=" + captureOutput +
                ", arguments=" + (arguments == null ? null : Arrays.asList(arguments)) +
                '}';
    }

    public String toXMLString() {
        StringBuilder actionXML = new StringBuilder(8192);
        actionXML.append("<action name=\"").append(name).append("\">");

        actionXML.append("<java>");
        actionXML.append("<job-tracker>").append(jobTrackerURI).append("</job-tracker>");
        actionXML.append("<name-node>").append(nameNodeURI).append("</name-node>");

        actionXML.append("<main-class>").append(clazzFQN).append("</main-class>");
        for (String argument : arguments) {
            actionXML.append("<arg>").append(argument).append("</arg>");
        }

        if (captureOutput) actionXML.append("<capture-output/>");
        actionXML.append("</java>");

        actionXML.append("<ok to=\"end\"/>");
        actionXML.append("<error to=\"fail\"/>");

        actionXML.append("</action>");
        return actionXML.toString();
    }
}
