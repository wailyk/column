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
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.AbstractColumnTupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.ColumnAwareDiskOnlyMultiComparator;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;

public final class LSMColumnBTreeRangeSearchCursor extends LSMBTreeRangeSearchCursor {
    private final List<AbstractColumnTupleReference> componentTupleList;

    public LSMColumnBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false, NoOpIndexCursorStats.INSTANCE);
    }

    public LSMColumnBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples,
            IIndexCursorStats stats) {
        super(opCtx, returnDeletedTuples, stats);
        componentTupleList = new ArrayList<>();
    }

    @Override
    protected BTreeAccessor createAccessor(LSMComponentType type, BTree btree, int index) throws HyracksDataException {
        if (type == LSMComponentType.MEMORY) {
            return super.createAccessor(type, btree, index);
        }
        ColumnBTree columnBTree = (ColumnBTree) btree;
        LSMColumnBTreeOpContext columnOpCtx = (LSMColumnBTreeOpContext) opCtx;
        return columnBTree.createAccessor(iap, index, columnOpCtx.createProjectionInfo());
    }

    @Override
    protected IIndexCursor createCursor(LSMComponentType type, BTreeAccessor accessor) {
        if (type == LSMComponentType.MEMORY) {
            return super.createCursor(type, accessor);
        }
        ColumnBTreeRangeSearchCursor cursor = (ColumnBTreeRangeSearchCursor) accessor.createSearchCursor(false);
        componentTupleList.add((AbstractColumnTupleReference) cursor.doGetTuple());
        return cursor;
    }

    @Override
    protected void markAsDeleted(PriorityQueueElement e) throws HyracksDataException {
        if (isMemoryComponent[e.getCursorIndex()]) {
            super.markAsDeleted(e);
            return;
        }
        AbstractColumnTupleReference columnTuple = (AbstractColumnTupleReference) e.getTuple();
        columnTuple.skip(1);
    }

    @Override
    protected void setPriorityQueueComparator() {
        if (!includeMutableComponent) {
            cmp = new ColumnAwareDiskOnlyMultiComparator(cmp);
        }
        if (pqCmp == null || cmp != pqCmp.getMultiComparator()) {
            pqCmp = new PriorityQueueComparator(cmp);
        }
    }

    @Override
    protected void excludeMemoryComponent() {
        //Replace the comparator with disk only comparator
        pqCmp.setMultiComparator(new ColumnAwareDiskOnlyMultiComparator(cmp));
    }

    @Override
    protected int replaceFrom() {
        //Disable replacing the in-memory component to disk component as the schema may change
        //TODO at least allow the replacement when no schema changes occur
        return -1;
    }

    /**
     * @return we need the tuple references for vertical merges
     */
    public List<AbstractColumnTupleReference> getComponentTupleList() {
        return componentTupleList;
    }
}
