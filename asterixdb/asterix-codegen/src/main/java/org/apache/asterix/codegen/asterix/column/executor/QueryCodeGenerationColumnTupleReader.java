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

import org.apache.asterix.column.metadata.AbstractColumnImmutableReadMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.AbstractColumnTupleReference;

public class QueryCodeGenerationColumnTupleReader extends AbstractColumnTupleReader {
    private final QueryCodeGenerationColumnMetadata columnMetadata;

    QueryCodeGenerationColumnTupleReader(AbstractColumnImmutableReadMetadata columnMetadata) {
        this.columnMetadata = (QueryCodeGenerationColumnMetadata) columnMetadata;
    }

    @Override
    public AbstractColumnTupleReference createTupleReference(ColumnBTreeReadLeafFrame frame, int componentIndex,
            IColumnReadMultiPageOp multiPageOp) {
        return new QueryCodeGenerationColumnTupleReference(componentIndex, frame, columnMetadata, multiPageOp);
    }
}
