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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.AbstractColumnTupleReference;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class LSMColumnBatchPointSearchCursor extends LSMBTreeBatchPointSearchCursor {
    private final List<AbstractColumnTupleReference> componentTupleList;

    public LSMColumnBatchPointSearchCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx);
        componentTupleList = new ArrayList<>();
    }

    @Override
    protected BTreeAccessor createAccessor(LSMComponentType type, BTree btree, int index) throws HyracksDataException {
        if (type == LSMComponentType.MEMORY) {
            return super.createAccessor(type, btree, index);
        }
        ColumnBTree columnBTree = (ColumnBTree) btree;
        LSMColumnBTreeOpContext columnOpCtx = (LSMColumnBTreeOpContext) opCtx;
        return columnBTree.createAccessor(NoOpIndexAccessParameters.INSTANCE, index,
                columnOpCtx.createProjectionInfo());
    }

    @Override
    protected ITreeIndexCursor createCursor(LSMComponentType type, BTreeAccessor accessor) {
        if (type == LSMComponentType.MEMORY) {
            return super.createCursor(type, accessor);
        }
        ColumnBTreeRangeSearchCursor cursor = (ColumnBTreeRangeSearchCursor) accessor.createPointCursor(false, true);
        componentTupleList.add((AbstractColumnTupleReference) cursor.doGetTuple());
        return cursor;
    }

    /**
     * @return we need the tuple references for vertical merges
     */
    public List<AbstractColumnTupleReference> getComponentTupleList() {
        return componentTupleList;
    }
}
