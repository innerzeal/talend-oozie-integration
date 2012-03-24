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

public class Workflow {

    private String name;
    private JavaAction action;

    public Workflow(String name, JavaAction action) {
        this.name = name;
        this.action = action;
    }

    public String getName() {
        return name;
    }

    public JavaAction getAction() {
        return action;
    }

    @Override
    public String toString() {
        return "Workflow{" +
                "name='" + name + '\'' +
                ", action=" + action +
                '}';
    }

    public String toXMLString() {
        StringBuilder workflowXML = new StringBuilder(8192);
        workflowXML.append("<workflow-app xmlns=\"uri:oozie:workflow:0.2\" name=\"").append(name).append("\">");
        workflowXML.append("<start to=\"").append(action.getName()).append("\"/>");

        workflowXML.append(action.toXMLString());

        workflowXML.append("<kill name=\"fail\"><message>Job failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message></kill>");

        workflowXML.append("<end name=\"end\"/>");
        workflowXML.append("</workflow-app>");

        return workflowXML.toString();
    }
}
