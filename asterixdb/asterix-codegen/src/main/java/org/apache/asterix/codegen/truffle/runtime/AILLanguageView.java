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

import static com.oracle.truffle.api.CompilerDirectives.shouldNotReachHere;

import org.apache.asterix.codegen.truffle.AILLanguage;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.ExplodeLoop;

/**
 * Language views are needed in order to allow tools to have a consistent perspective on primitive
 * or foreign values from the perspective of this language. The interop interpretation for primitive
 * values like Integer or String is not language specific by default. Therefore this language view
 * calls routines to print such values the SimpleLanguage way. It is important to note that language
 * views are not passed as normal values through the interpreter execution. It is designed only as a
 * temporary helper for tools.
 * <p>
 * There is more information in {@link TruffleLanguage#getLanguageView(Object, Object)}
 */
@ExportLibrary(value = InteropLibrary.class, delegateTo = "delegate")
@SuppressWarnings("static-method")
public final class AILLanguageView implements TruffleObject {

    final Object delegate;

    AILLanguageView(Object delegate) {
        this.delegate = delegate;
    }

    @ExportMessage
    boolean hasLanguage() {
        return true;
    }

    /*
     * Language views must always associate with the language they were created for. This allows
     * tooling to take a primitive or foreign value and create a value of simple language of it.
     */
    @ExportMessage
    Class<? extends TruffleLanguage<?>> getLanguage() {
        return AILLanguage.class;
    }

    @ExportMessage
    @ExplodeLoop
    boolean hasMetaObject(@CachedLibrary("this.delegate") InteropLibrary interop) {
        /*
         * We use the isInstance method to find out whether one of the builtin simple language types
         * apply. If yes, then we can provide a meta object in getMetaObject. The interop contract
         * requires to be precise.
         *
         * Since language views are only created for primitive values and values of other languages,
         * values from simple language itself directly implement has/getMetaObject. For example
         * SLFunction is already associated with the SLLanguage and therefore the language view will
         * not be used.
         */
        for (AILType type : AILType.PRECEDENCE) {
            if (type.isInstance(delegate, interop)) {
                return true;
            }
        }
        return false;
    }

    @ExportMessage
    @ExplodeLoop
    Object getMetaObject(@CachedLibrary("this.delegate") InteropLibrary interop) throws UnsupportedMessageException {
        /*
         * We do the same as in hasMetaObject but actually return the type this time.
         */
        for (AILType type : AILType.PRECEDENCE) {
            if (type.isInstance(delegate, interop)) {
                return type;
            }
        }
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    @ExplodeLoop
    Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects,
            @CachedLibrary("this.delegate") InteropLibrary interop) {
        for (AILType type : AILType.PRECEDENCE) {
            if (type.isInstance(this.delegate, interop)) {
                try {
                    /*
                     * The type is a partial evaluation constant here as we use @ExplodeLoop. So
                     * this if-else cascade should fold after partial evaluation.
                     */
                    if (type == AILType.INT) {
                        return longToString(interop.asLong(delegate));
                    } else if (type == AILType.DOUBLE) {
                        return doubleToString(interop.asDouble(delegate));
                    } else if (type == AILType.BOOLEAN) {
                        return Boolean.toString(interop.asBoolean(delegate));
                    } else if (type == AILType.STRING) {
                        return interop.asString(delegate);
                    } else {
                        /* We use the type name as fallback for any other type */
                        return type.getName();
                    }
                } catch (UnsupportedMessageException e) {
                    throw shouldNotReachHere(e);
                }
            }
        }
        return "Unsupported";
    }

    /*
     * Long.toString is not safe for partial evaluation and therefore needs to be called behind a
     * boundary.
     */
    @TruffleBoundary
    private static String longToString(long l) {
        return Long.toString(l);
    }

    @TruffleBoundary
    private static String doubleToString(double d) {
        return Double.toString(d);
    }

    public static Object create(Object value) {
        assert isPrimitiveOrFromOtherLanguage(value);
        return new AILLanguageView(value);
    }

    /*
     * Language views are intended to be used only for primitives and other language values.
     */
    private static boolean isPrimitiveOrFromOtherLanguage(Object value) {
        InteropLibrary interop = InteropLibrary.getFactory().getUncached(value);
        try {
            return !interop.hasLanguage(value) || interop.getLanguage(value) != AILLanguage.class;
        } catch (UnsupportedMessageException e) {
            throw shouldNotReachHere(e);
        }
    }

    /**
     * Returns a language view for primitive or foreign values. Returns the same value for values
     * that are already originating from SimpleLanguage. This is useful to view values from the
     * perspective of simple language in slow paths, for example, printing values in error messages.
     */
    @TruffleBoundary
    public static Object forValue(Object value) {
        if (value == null) {
            return null;
        }
        InteropLibrary lib = InteropLibrary.getFactory().getUncached(value);
        try {
            if (lib.hasLanguage(value) && lib.getLanguage(value) == AILLanguage.class) {
                return value;
            } else {
                return create(value);
            }
        } catch (UnsupportedMessageException e) {
            throw shouldNotReachHere(e);
        }
    }

}
