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

import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.codegen.asterix.column.reader.AbstractTypedColumnReader;
import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.runtime.cursor.AILIndexCursor;
import org.apache.asterix.codegen.truffle.runtime.reader.column.AILColumnReader;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnCodeGenerationExecutor;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeSearchCursor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;

public final class ColumnCodeGenerationExecutor implements IColumnCodeGenerationExecutor {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final Map<String, String> OPTIONS = new HashMap<>();
    private final IHyracksTaskContext context;
    private final ARecordType datasetType;
    private final int numberOfPrimaryKeys;
    private final ARecordType[] paths;
    private final Map<String, FunctionCallInformation> functionCallInfoMap;
    private final IWarningCollector warningCollector;
    private final ATypeTag[] typeTags;
    private final String functionName;
    private final String code;

    static {
        if (ManagementFactory.getRuntimeMXBean().getInputArguments().contains("-ea")) {
            OPTIONS.put("engine.TraceCompilation", "true");
            OPTIONS.put("cpusampler", "true");
            OPTIONS.put("cpusampler.Delay", "1000");
            OPTIONS.put("engine.TracePerformanceWarnings", "all");
        }
    }

    ColumnCodeGenerationExecutor(IHyracksTaskContext context, ARecordType datasetType, int numberOfPrimaryKeys,
            ARecordType[] paths, Map<String, FunctionCallInformation> functionCallInfoMap,
            IWarningCollector warningCollector, String functionName, String code) {
        this.context = context;
        this.datasetType = datasetType;
        this.numberOfPrimaryKeys = numberOfPrimaryKeys;
        this.paths = paths;
        this.functionCallInfoMap = functionCallInfoMap;
        this.warningCollector = warningCollector;
        typeTags = new ATypeTag[paths.length];
        Arrays.fill(typeTags, ATypeTag.MISSING);
        this.functionName = functionName;
        this.code = code;
    }

    @Override
    public void execute(IIndexCursor cursor, DataOutput dos, ArrayTupleBuilder tb, IFrameWriter writer,
            FrameTupleAppender appender) throws HyracksDataException {
        try (Context truffleContext = Context.newBuilder(AILLanguage.ID).options(OPTIONS)
                .allowHostAccess(HostAccess.ALL).allowExperimentalOptions(true).build()) {
            Object[] arguments = getArguments(cursor, dos, tb, writer, appender);
            truffleContext.eval(AILLanguage.ID, code);
            Value execute = truffleContext.getBindings(AILLanguage.ID).getMember(functionName);
            execute.executeVoid(arguments);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }

        //        Context truffleContext = Context.newBuilder(AILLanguage.ID).options(OPTIONS).allowHostAccess(HostAccess.ALL)
        //                .allowExperimentalOptions(true).build();
        //        Object[] arguments = getArguments(cursor, dos, tb, writer, appender);
        //        executeJava(arguments);
    }

    @Override
    public IColumnProjectionInfo createProjectionInfo(IValueReference columnMetadata) throws HyracksDataException {
        try {
            QueryCodeGenerationColumnMetadata metadata =
                    QueryCodeGenerationColumnMetadata.create(datasetType, numberOfPrimaryKeys, columnMetadata,
                            new ColumnValueReaderFactory(), paths, functionCallInfoMap, warningCollector);
            IColumnValuesReader[] readers = metadata.getReaders();
            for (int i = 0; i < readers.length; i++) {
                typeTags[i] = readers[i].getTypeTag();
            }
            return metadata;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void executeJava(Object[] arguments) throws HyracksDataException {
        LOGGER.fatal("JAVA");
        AILIndexCursor cursor = (AILIndexCursor) arguments[0];
        AILResultWriter resultWriter = (AILResultWriter) arguments[1];
        //        AILColumnReader reader = (AILColumnReader) arguments[2];
        long maxVal = 0;
        while (cursor.next()) {
            //            reader.next();
            //            while (!reader.isEndOfArray()) {
            //                //                maxVal = Math.max(maxVal, reader.getLong());
            //
            //                //                maxVal += reader.getReader().isNull() || reader.getReader().isMissing() ? 0 : 1;
            //                maxVal += reader.getReader().isNull() || reader.getReader().isMissing() ? 0 : 1;
            //                reader.next();
            //            }
            maxVal++;
        }
        resultWriter.append(maxVal);
        resultWriter.flush();
    }

    private Object[] getArguments(IIndexCursor cursor, DataOutput dos, ArrayTupleBuilder tb, IFrameWriter writer,
            FrameTupleAppender appender) {
        LSMColumnBTreeRangeSearchCursor columnCursor =
                (LSMColumnBTreeRangeSearchCursor) ((LSMBTreeSearchCursor) cursor).getCurrentCursor();
        //1 for cursor and 1 for resultWriter and the rest are for readers
        Object[] arguments = new Object[1 + 1 + typeTags.length];
        AbstractTypedColumnReader[] readers = new AbstractTypedColumnReader[typeTags.length];
        for (int i = 0; i < typeTags.length; i++) {
            AbstractTypedColumnReader typedReader = AbstractTypedColumnReader.createReader(typeTags[i]);
            arguments[i + 2] = new AILColumnReader(typedReader);
            readers[i] = typedReader;
        }
        arguments[0] = new AILIndexCursor(columnCursor, readers);
        arguments[1] = new AILResultWriter(context, dos, tb, writer, appender);
        return arguments;
    }
}
