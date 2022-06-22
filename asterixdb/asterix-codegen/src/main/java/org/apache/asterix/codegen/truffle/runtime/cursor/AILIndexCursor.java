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
package org.apache.asterix.codegen.truffle.runtime.cursor;

import org.apache.asterix.codegen.asterix.column.executor.QueryCodeGenerationColumnTupleReference;
import org.apache.asterix.codegen.asterix.column.reader.AbstractTypedColumnReader;
import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.AILRuntimeException;
import org.apache.asterix.codegen.truffle.runtime.AILMemberUtil;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTreeRangeSearchCursor;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;

@ExportLibrary(InteropLibrary.class)
public final class AILIndexCursor implements TruffleObject {

    private static final String[] MEMBERS = { AILMemberUtil.NEXT };
    private final LSMColumnBTreeRangeSearchCursor cursor;
    private final AbstractTypedColumnReader[] readers;
    private int numberOfTuples;

    public AILIndexCursor(LSMColumnBTreeRangeSearchCursor cursor, AbstractTypedColumnReader[] readers) {
        this.cursor = cursor;
        this.readers = readers;
        numberOfTuples = 0;
    }

    @ExportMessage
    Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects) {
        return "Index Cursor";
    }

    @ExportMessage
    boolean hasLanguage() {
        return true;
    }

    @ExportMessage
    Class<? extends TruffleLanguage<?>> getLanguage() {
        return AILLanguage.class;
    }

    @TruffleBoundary
    public boolean next() {
        try {
            if (numberOfTuples > 0) {
                numberOfTuples--;
                setReaders(cursor);
                return true;
            }
            final boolean hasNext = cursor.hasNext();
            if (hasNext) {
                cursor.next();
                numberOfTuples = setReaders(cursor);
            }
            return hasNext;
        } catch (HyracksDataException e) {
            throw new AILRuntimeException();
        }
    }

    @ExplodeLoop
    private int setReaders(LSMColumnBTreeRangeSearchCursor cursor) throws HyracksDataException {
        QueryCodeGenerationColumnTupleReference tuple = (QueryCodeGenerationColumnTupleReference) cursor.doGetTuple();
        tuple.consume();
        if (readers.length > 0) {
            IColumnValuesReader[] columnReaders = tuple.getReaders();
            for (int i = 0; i < readers.length; i++) {
                readers[i].setReader(columnReaders[i]);
            }
        }
        return tuple.getTupleCount() - 1;
    }

    /*
     * ***********************************************
     * Members stuff
     * ***********************************************
     */

    @ExportMessage
    boolean hasMembers() {
        return true;
    }

    @ExportMessage
    Object[] getMembers(boolean includeInternal) {
        return MEMBERS;
    }

    @ExportMessage
    boolean isMemberReadable(String name) {
        return AILMemberUtil.NEXT.equals(name);
    }

    @ExportMessage
    Object readMember(String name) {
        return new AILIndexCursorNextCall(this);
    }

    public QueryCodeGenerationColumnTupleReference getTuple() {
        return (QueryCodeGenerationColumnTupleReference) cursor.getTuple();
    }

    public abstract static class NextNode extends Node {
        public abstract boolean execute(AILIndexCursor call);

        @Specialization
        boolean next(AILIndexCursor cursor) {
            return cursor.next();
        }
    }
}
