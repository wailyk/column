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
package org.apache.asterix.codegen.truffle.nodes.interop;

import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.InvalidArrayIndexException;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.profiles.BranchProfile;

@ExportLibrary(InteropLibrary.class)
public final class NodeObjectDescriptorKeys implements TruffleObject {

    private final String keyName;

    NodeObjectDescriptorKeys(String keyName) {
        this.keyName = keyName;
    }

    @ExportMessage
    @SuppressWarnings("static-method")
    boolean hasArrayElements() {
        return true;
    }

    @ExportMessage
    @SuppressWarnings("static-method")
    boolean isArrayElementReadable(long index) {
        return index >= 0 && index < 1;
    }

    @ExportMessage
    @SuppressWarnings("static-method")
    long getArraySize() {
        return 1;
    }

    @ExportMessage
    Object readArrayElement(long index, @Cached BranchProfile exception) throws InvalidArrayIndexException {
        if (!isArrayElementReadable(index)) {
            exception.enter();
            throw InvalidArrayIndexException.create(index);
        }
        return keyName;
    }

}
