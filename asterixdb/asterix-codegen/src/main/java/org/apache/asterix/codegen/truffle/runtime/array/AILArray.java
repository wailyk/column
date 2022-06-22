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
package org.apache.asterix.codegen.truffle.runtime.array;

import org.apache.asterix.codegen.truffle.AILRuntimeException;
import org.apache.asterix.codegen.truffle.runtime.AILMemberUtil;
import org.apache.asterix.codegen.truffle.runtime.array.member.ArrayAppend;
import org.apache.asterix.codegen.truffle.runtime.array.member.ArrayCount;
import org.apache.asterix.codegen.truffle.runtime.array.member.ArrayDistinct;
import org.apache.asterix.codegen.truffle.runtime.array.member.ArrayPairs;
import org.apache.asterix.codegen.truffle.runtime.array.member.ArrayReset;
import org.apache.asterix.codegen.truffle.runtime.array.member.ArraySort;
import org.apache.asterix.codegen.truffle.runtime.array.storage.AbstractArrayStorage;

import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.ExplodeLoop;

@ExportLibrary(InteropLibrary.class)
public final class AILArray implements TruffleObject {
    static final String[] MEMBERS =
            { AILMemberUtil.NEXT, AILMemberUtil.IS_END_OF_ARRAY, AILMemberUtil.GET_VALUE, AILMemberUtil.TO_ARRAY };
    private final AbstractArrayStorage storage;

    public AILArray(AbstractArrayStorage storage) {
        this.storage = storage;
    }

    public AbstractArrayStorage getStorage() {
        return storage;
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

    @ExportMessage
    public Object readMember(String name) {
        switch (name) {
            case ArrayAppend.APPEND:
                return new ArrayAppend(this);
            case ArrayReset.RESET:
                return new ArrayReset(this);
            case ArrayCount.COUNT:
                return new ArrayCount(this);
            case ArrayPairs.PAIRS:
                return new ArrayPairs(this);
            case ArraySort.SORT:
                return new ArraySort(this);
            case ArrayDistinct.DISTINCT:
                return new ArrayDistinct(this);
            default:
                throw new AILRuntimeException();
        }
    }

    public AILArray createPairArray() {
        return new AILArray(storage.createPairArray());
    }
}
