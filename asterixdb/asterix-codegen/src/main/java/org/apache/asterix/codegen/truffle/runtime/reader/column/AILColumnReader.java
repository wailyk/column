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
package org.apache.asterix.codegen.truffle.runtime.reader.column;

import org.apache.asterix.codegen.asterix.column.reader.AbstractTypedColumnReader;
import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.runtime.AILMemberUtil;
import org.apache.asterix.codegen.truffle.runtime.AILType;
import org.apache.asterix.codegen.truffle.runtime.reader.column.member.AILReaderGetValueCall;
import org.apache.asterix.codegen.truffle.runtime.reader.column.member.AILReaderIsEndOfArrayCall;
import org.apache.asterix.codegen.truffle.runtime.reader.column.member.AILReaderNextCall;
import org.apache.asterix.codegen.truffle.runtime.reader.column.member.AILReaderRewindCall;
import org.apache.asterix.codegen.truffle.runtime.reader.column.member.AILReaderToArrayCall;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.ExplodeLoop;

@ExportLibrary(InteropLibrary.class)
public final class AILColumnReader implements TruffleObject {
    static final String[] MEMBERS = { AILMemberUtil.NEXT, AILMemberUtil.IS_END_OF_ARRAY, AILMemberUtil.GET_VALUE,
            AILMemberUtil.TO_ARRAY, AILMemberUtil.REWIND };

    private final AbstractTypedColumnReader reader;

    public AILColumnReader(AbstractTypedColumnReader reader) {
        this.reader = reader;
    }

    @ExportMessage
    Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects) {
        return "Column Typed Reader";
    }

    @ExportMessage
    boolean hasLanguage() {
        return true;
    }

    @ExportMessage
    Class<? extends TruffleLanguage<?>> getLanguage() {
        return AILLanguage.class;
    }

    @ExportMessage
    boolean hasMetaObject() {
        return true;
    }

    @ExportMessage
    Object getMetaObject() {
        return AILType.COLUMN_READER;
    }

    @ExportMessage
    boolean hasMembers() {
        return true;
    }

    @ExportMessage
    Object[] getMembers(boolean includeInternal) {
        return MEMBERS;
    }

    @ExportMessage
    @ExplodeLoop
    boolean isMemberReadable(String name) {
        for (String member : MEMBERS) {
            if (member.equals(name)) {
                return true;
            }
        }
        return false;
    }

    public AbstractTypedColumnReader getReader() {
        return reader;
    }

    public void next() {
        reader.next();
    }

    public boolean isEndOfArray() {
        return reader.isEndOfArray();
    }

    @ExportMessage
    public Object readMember(String name) {
        switch (name) {
            case AILMemberUtil.NEXT:
                return new AILReaderNextCall(reader);
            case AILMemberUtil.IS_END_OF_ARRAY:
                return new AILReaderIsEndOfArrayCall(reader);
            case AILMemberUtil.TO_ARRAY:
                return new AILReaderToArrayCall(reader);
            case AILMemberUtil.REWIND:
                return new AILReaderRewindCall(reader);
            default:
                return new AILReaderGetValueCall(reader);
        }
    }

}