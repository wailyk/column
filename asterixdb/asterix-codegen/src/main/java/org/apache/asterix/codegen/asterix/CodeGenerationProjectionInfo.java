/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.codegen.asterix;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.algebricks.core.algebra.metadata.projection.IProjectionInfo;
import org.apache.hyracks.algebricks.core.algebra.metadata.projection.ProjectorType;

public class CodeGenerationProjectionInfo implements IProjectionInfo<ARecordType[]> {
    private final String functionName;
    private final String code;
    private final ARecordType[] paths;
    private final Map<String, FunctionCallInformation> functionCallInfoMap;

    public CodeGenerationProjectionInfo(String functionName, String code, ARecordType[] paths,
            Map<String, FunctionCallInformation> functionCallInfoMap) {
        this.functionName = functionName;
        this.code = code;
        this.paths = paths;
        this.functionCallInfoMap = functionCallInfoMap;
    }

    private CodeGenerationProjectionInfo(CodeGenerationProjectionInfo other) {
        this.functionName = other.functionName;
        this.code = other.code;
        ARecordType[] copyPaths = other.paths;
        paths = new ARecordType[copyPaths.length];
        System.arraycopy(copyPaths, 0, paths, 0, copyPaths.length);
        functionCallInfoMap = new HashMap<>(other.functionCallInfoMap);
    }

    @Override
    public ARecordType[] getProjectionInfo() {
        return paths;
    }

    @Override
    public ProjectorType getProjectorType() {
        return ProjectorType.COMPILED;
    }

    @Override
    public IProjectionInfo<ARecordType[]> createCopy() {
        return new CodeGenerationProjectionInfo(this);
    }

    public String getCode() {
        return code;
    }

    public String getFunctionName() {
        return functionName;
    }

    public Map<String, FunctionCallInformation> getFunctionCallInfoMap() {
        return functionCallInfoMap;
    }

    @Override
    public String toString() {
        return code;
    }
}
