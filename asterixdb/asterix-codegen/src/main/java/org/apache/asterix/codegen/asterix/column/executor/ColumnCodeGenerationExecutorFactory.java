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
package org.apache.asterix.codegen.asterix.column.executor;

import java.util.Map;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.common.projection.ICodeGenerationExecutor;
import org.apache.hyracks.storage.common.projection.ICodeGenerationExecutorFactory;

public class ColumnCodeGenerationExecutorFactory implements ICodeGenerationExecutorFactory {
    private static final long serialVersionUID = 6586388689265604419L;
    private final String functionName;
    private final String code;

    private final ARecordType datasetType;
    private final int numberOfPrimaryKeys;
    private final ARecordType[] paths;
    private final Map<String, FunctionCallInformation> functionCallInfoMap;

    public ColumnCodeGenerationExecutorFactory(ARecordType datasetType, int numberOfPrimaryKeys, ARecordType[] paths,
            Map<String, FunctionCallInformation> functionCallInfoMap, String functionName, String code) {
        this.datasetType = datasetType;
        this.numberOfPrimaryKeys = numberOfPrimaryKeys;
        this.paths = paths;
        this.functionCallInfoMap = functionCallInfoMap;

        this.functionName = functionName;
        this.code = code;
    }

    @Override
    public ICodeGenerationExecutor createExecutor(IHyracksTaskContext context) {
        return new ColumnCodeGenerationExecutor(context, datasetType, numberOfPrimaryKeys, paths, functionCallInfoMap,
                context.getWarningCollector(), functionName, code);
    }
}
