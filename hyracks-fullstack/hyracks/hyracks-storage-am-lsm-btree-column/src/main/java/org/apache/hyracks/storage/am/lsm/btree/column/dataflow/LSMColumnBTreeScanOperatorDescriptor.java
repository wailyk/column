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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.common.projection.ICodeGenerationExecutorFactory;

public class LSMColumnBTreeScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final int[] lowKeyFields;
    protected final int[] highKeyFields;
    protected final boolean lowKeyInclusive;
    protected final boolean highKeyInclusive;
    protected final IIndexDataflowHelperFactory indexHelperFactory;
    protected final boolean retainInput;
    protected final boolean retainMissing;
    protected final IMissingWriterFactory missingWriterFactory;
    protected final ISearchOperationCallbackFactory searchCallbackFactory;
    protected final IMissingWriterFactory nonFilterWriterFactory;
    protected final ICodeGenerationExecutorFactory codeGenExecutorFactory;

    public LSMColumnBTreeScanOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            IMissingWriterFactory nonFilterWriterFactory, ICodeGenerationExecutorFactory codeGenExecutorFactory) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.retainInput = retainInput;
        this.retainMissing = retainMissing;
        this.missingWriterFactory = missingWriterFactory;
        this.searchCallbackFactory = searchCallbackFactory;
        this.lowKeyFields = lowKeyFields;
        this.highKeyFields = highKeyFields;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
        this.nonFilterWriterFactory = nonFilterWriterFactory;
        this.outRecDescs[0] = outRecDesc;
        this.codeGenExecutorFactory = codeGenExecutorFactory;
    }

    @Override
    public LSMColumnBTreeScanOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new LSMColumnBTreeScanOperatorNodePushable(ctx, partition,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), indexHelperFactory,
                searchCallbackFactory, codeGenExecutorFactory);
    }

    @Override
    public String getDisplayName() {
        return "BTree Search";
    }

}
