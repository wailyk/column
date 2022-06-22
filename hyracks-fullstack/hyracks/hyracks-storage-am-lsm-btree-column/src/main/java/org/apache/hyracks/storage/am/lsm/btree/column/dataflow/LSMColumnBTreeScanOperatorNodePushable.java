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
package org.apache.hyracks.storage.am.lsm.btree.column.dataflow;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.impls.NoOpTupleProjectorFactory;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.projection.ICodeGenerationExecutor;
import org.apache.hyracks.storage.common.projection.ICodeGenerationExecutorFactory;

public class LSMColumnBTreeScanOperatorNodePushable extends IndexSearchOperatorNodePushable {
    private static final RangePredicate SCAN_PREDICATE = new RangePredicate();
    private final ICodeGenerationExecutor codeGenExecutor;

    public LSMColumnBTreeScanOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            IIndexDataflowHelperFactory indexHelperFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            ICodeGenerationExecutorFactory codeGenFactory) throws HyracksDataException {
        super(ctx, inputRecDesc, partition, null, null, indexHelperFactory, false, false, null, searchCallbackFactory,
                false, null, null, -1, false, null, null, NoOpTupleProjectorFactory.INSTANCE);
        codeGenExecutor = codeGenFactory.createExecutor(ctx);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        indexAccessor.search(cursor, searchPred);
        codeGenExecutor.execute(cursor, dos, tb, writer, appender);
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        //NoOp
    }

    @Override
    protected ISearchPredicate createSearchPredicate() {
        return SCAN_PREDICATE;
    }

    @Override
    protected int getFieldCount() {
        //Not used
        return 0;
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) throws HyracksDataException {
        //Set projection info provider (used only by LSMColumnBTree)
        iap.getParameters().put(HyracksConstants.PROJECTION_INFO_PROVIDER, codeGenExecutor);
    }

}
