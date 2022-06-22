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

import java.nio.ByteBuffer;

import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.tuple.AbstractAsterixColumnTupleReference;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

public final class QueryCodeGenerationColumnTupleReference extends AbstractAsterixColumnTupleReference {
    private final IColumnValuesReader[] readers;

    QueryCodeGenerationColumnTupleReference(int componentIndex, ColumnBTreeReadLeafFrame frame,
            QueryCodeGenerationColumnMetadata info, IColumnReadMultiPageOp multiPageOp) {
        super(componentIndex, frame, info, multiPageOp);
        readers = info.getReaders();
    }

    public IColumnValuesReader[] getReaders() {
        return readers;
    }

    @Override
    protected IColumnValuesReader[] getPrimaryKeyReaders(IColumnProjectionInfo info) {
        return ((QueryCodeGenerationColumnMetadata) info).getPrimaryKeyReaders();
    }

    @Override
    protected boolean startNewPage(ByteBuffer pageZero, int numberOfColumns, int numberOfTuples) {
        //Skip filters
        pageZero.position(pageZero.position() + numberOfColumns * AbstractColumnFilterWriter.FILTER_SIZE);
        return true;
    }

    @Override
    protected void startColumn(IColumnBufferProvider buffersProvider, int startIndex, int ordinal, int numberOfTuples)
            throws HyracksDataException {
        AbstractBytesInputStream columnStream = columnStreams[ordinal];
        columnStream.reset(buffersProvider);
        IColumnValuesReader reader = readers[ordinal];
        reader.reset(columnStream, numberOfTuples);
        reader.skip(startIndex);
    }

    @Override
    public void skip(int count) throws HyracksDataException {
        for (int i = 0; i < readers.length; i++) {
            readers[i].skip(count);
        }
    }
}
