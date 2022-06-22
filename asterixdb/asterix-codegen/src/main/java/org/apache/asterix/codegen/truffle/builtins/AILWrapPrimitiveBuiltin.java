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
package org.apache.asterix.codegen.truffle.builtins;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.library.Message;
import com.oracle.truffle.api.library.ReflectionLibrary;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Builtin function to wrap primitive values in order to increase coverage of the Truffle TCK test.
 */
@NodeInfo(shortName = "wrapPrimitive")
@SuppressWarnings("unused")
public abstract class AILWrapPrimitiveBuiltin extends AILBuiltinNode {

    @TruffleBoundary
    @Specialization
    public Object doDefault(Object value) {
        if (value instanceof PrimitiveValueWrapper) {
            return value;
        } else {
            return new PrimitiveValueWrapper(value);
        }
    }

    @ExportLibrary(ReflectionLibrary.class)
    static final class PrimitiveValueWrapper implements TruffleObject {

        final Object delegate;

        PrimitiveValueWrapper(Object delegate) {
            this.delegate = delegate;
        }

        @ExportMessage
        Object send(Message message, Object[] args, @CachedLibrary("this.delegate") ReflectionLibrary reflection)
                throws Exception {
            return reflection.send(this.delegate, message, args);
        }

    }

}
