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
package org.apache.asterix.codegen.truffle.runtime;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.util.string.UTF8StringUtil;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

/**
 * StringRuntime is different from Java string, which is used to declare function names and variables names.
 * StringRuntime is an internal AsterixDB serialized version of a string, see {@link UTF8StringPointable}.
 */
@ExportLibrary(InteropLibrary.class)
public class AILStringRuntime implements TruffleObject {
    private final VoidPointable stringValue;

    public AILStringRuntime() {
        stringValue = new VoidPointable();
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
        return AILType.STRING_RUNTIME;
    }

    @ExportMessage
    Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects) {
        return "Asterix String";
    }

    public IValueReference getStringValue() {
        return stringValue;
    }

    public void reset(AILStringRuntime value) {
        stringValue.set(value.getStringValue());
    }

    public void reset(IValueReference value) {
        stringValue.set(value);
    }

    public void reset(GrowableArray value) {
        stringValue.set(value.getByteArray(), 0, value.getLength());
    }

    @TruffleBoundary
    public boolean isEqual(AILStringRuntime right) {
        return UTF8StringPointable.areEqual(stringValue, right.stringValue);
    }

    @TruffleBoundary
    public int compare(AILStringRuntime right) {
        return UTF8StringPointable.compare(stringValue, right.stringValue);
    }

    public int getLength() {
        return UTF8StringUtil.getNumCodePoint(stringValue.getByteArray(), stringValue.getStartOffset());
    }

    public int getLengthInBytes() {
        return stringValue.getLength();
    }
}
