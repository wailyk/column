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
package org.apache.asterix.codegen.truffle.runtime.reader.column.member;

import org.apache.asterix.codegen.asterix.column.reader.AbstractTypedColumnReader;
import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.runtime.AILMissingRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILNullRuntime;
import org.apache.asterix.codegen.truffle.runtime.reader.column.member.ReaderNodes.GetValueNode;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.ReportPolymorphism;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.profiles.ConditionProfile;

@ExportLibrary(InteropLibrary.class)
@ReportPolymorphism
public final class AILReaderGetValueCall implements TruffleObject {
    private final AbstractTypedColumnReader reader;

    public AILReaderGetValueCall(AbstractTypedColumnReader reader) {
        this.reader = reader;
    }

    @ExportMessage
    Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects) {
        return "Index Cursor Next";
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
    boolean isExecutable() {
        return true;
    }

    @ExportMessage
    abstract static class Execute {
        @Specialization(guards = "isMissing(getValueCall, profile)")
        static AILMissingRuntime getMissing(AILReaderGetValueCall getValueCall, Object[] args,
                @Cached(allowUncached = true, value = "createProfile()") ConditionProfile profile) {
            return AILMissingRuntime.INSTANCE;
        }

        @Specialization(guards = "isNull(getValueCall, profile)")
        static AILNullRuntime getNull(AILReaderGetValueCall getValueCall, Object[] args,
                @Cached(allowUncached = true, value = "createProfile()") ConditionProfile profile) {
            return AILNullRuntime.INSTANCE;
        }

        @Specialization
        static Object getValue(AILReaderGetValueCall getValueCall, Object[] args, @Cached GetValueNode getValueNode) {
            return getValueNode.executeGeneric(getValueCall.reader);
        }

        static ConditionProfile createProfile() {
            return ConditionProfile.createCountingProfile();
        }

        static boolean isNull(AILReaderGetValueCall getValueCall, ConditionProfile profile) {
            return profile.profile(getValueCall.reader.isNull());
        }

        static boolean isMissing(AILReaderGetValueCall getValueCall, ConditionProfile profile) {
            return profile.profile(getValueCall.reader.isMissing());
        }
    }
}
