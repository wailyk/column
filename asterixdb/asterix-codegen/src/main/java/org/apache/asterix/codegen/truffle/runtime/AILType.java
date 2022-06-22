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
import org.apache.asterix.codegen.truffle.runtime.cursor.AILIndexCursor;
import org.apache.asterix.codegen.truffle.runtime.reader.column.AILColumnReader;

import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

/**
 * The builtin type definitions for SimpleLanguage. SL has no custom types, so it is not possible
 * for a guest program to create new instances of SLType.
 * <p>
 * The isInstance type checks are declared using an functional interface and are expressed using the
 * interoperability libraries. The advantage of this is type checks automatically work for foreign
 * values or primitive values like byte or short.
 * <p>
 * The class implements the interop contracts for {@link InteropLibrary#isMetaObject(Object)} and
 * {@link InteropLibrary#isMetaInstance(Object, Object)}. The latter allows other languages and
 * tools to perform type checks using types of simple language.
 * <p>
 * In order to assign types to guest language values, SL values implement
 * {@link InteropLibrary#getMetaObject(Object)}. The interop contracts for primitive values cannot
 * be overriden, so in order to assign meta-objects to primitive values, the primitive values are
 * assigned using language views. See {@link AILLanguage#getLanguageView}.
 */
@ExportLibrary(InteropLibrary.class)
@SuppressWarnings("static-method")
public final class AILType implements TruffleObject {

    /*
     * These are the sets of builtin types in simple languages. In case of simple language the types
     * nicely match those of the types in InteropLibrary. This might not be the case and more
     * additional checks need to be performed (similar to number checking for SLBigNumber).
     */
    public static final AILType INT = new AILType("Int", InteropLibrary::fitsInLong);
    public static final AILType DOUBLE = new AILType("DOUBLE", InteropLibrary::fitsInDouble);
    public static final AILType NULL = new AILType("NULL", InteropLibrary::isNull);
    public static final AILType STRING = new AILType("String", InteropLibrary::isString);
    public static final AILType STRING_RUNTIME = new AILType("StringRuntime", (l, v) -> v instanceof AILStringRuntime);
    public static final AILType BOOLEAN = new AILType("Boolean", InteropLibrary::isBoolean);
    public static final AILType FUNCTION = new AILType("Function", InteropLibrary::isExecutable);
    public static final AILType COLUMN_READER = new AILType("ColumnReader", (l, v) -> v instanceof AILColumnReader);
    public static final AILType CURSOR = new AILType("Cursor", (l, v) -> v instanceof AILIndexCursor);
    public static final AILType OBJECT = new AILType("Object", InteropLibrary::hasMembers);

    /*
     * This array is used when all types need to be checked in a certain order. While most interop
     * types like number or string are exclusive, others traits like members might not be. For
     * example, an object might be a function. In SimpleLanguage we decided to make functions,
     * functions and not objects.
     */
    @CompilationFinal(dimensions = 1)
    public static final AILType[] PRECEDENCE = new AILType[] { NULL, BOOLEAN, INT, DOUBLE, STRING, STRING_RUNTIME,
            FUNCTION, COLUMN_READER, CURSOR, OBJECT };

    private final String name;
    private final TypeCheck isInstance;

    /*
     * We don't allow dynamic instances of SLType. Real languages might want to expose this for
     * types that are user defined.
     */
    private AILType(String name, TypeCheck isInstance) {
        this.name = name;
        this.isInstance = isInstance;
    }

    /**
     * Checks whether this type is of a certain instance. If used on fast-paths it is required to
     * cast {@link AILType} to a constant.
     */
    public boolean isInstance(Object value, InteropLibrary interop) {
        CompilerAsserts.partialEvaluationConstant(this);
        return isInstance.check(interop, value);
    }

    @ExportMessage
    boolean hasLanguage() {
        return true;
    }

    @ExportMessage
    Class<? extends TruffleLanguage<?>> getLanguage() {
        return AILLanguage.class;
    }

    /*
     * All SLTypes are declared as interop meta-objects. Other example for meta-objects are Java
     * classes, or JavaScript prototypes.
     */
    @ExportMessage
    boolean isMetaObject() {
        return true;
    }

    /*
     * SL does not have the notion of a qualified or simple name, so we return the same type name
     * for both.
     */
    @ExportMessage(name = "getMetaQualifiedName")
    @ExportMessage(name = "getMetaSimpleName")
    public Object getName() {
        return name;
    }

    @ExportMessage(name = "toDisplayString")
    Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects) {
        return name;
    }

    @Override
    public String toString() {
        return "SLType[" + name + "]";
    }

    /*
     * The interop message isMetaInstance might be used from other languages or by the {@link
     * SLIsInstanceBuiltin isInstance} builtin. It checks whether a given value, which might be a
     * primitive, foreign or SL value is of a given SL type. This allows other languages to make
     * their instanceOf interopable with foreign values.
     */
    @ExportMessage
    static class IsMetaInstance {

        /*
         * We assume that the same type is checked at a source location. Therefore we use an inline
         * cache to specialize for observed types to be constant. The limit of "3" specifies that we
         * specialize for 3 different types until we rewrite to the doGeneric case. The limit in
         * this example is somewhat arbitrary and should be determined using careful tuning with
         * real world benchmarks.
         */
        @Specialization(guards = "type == cachedType", limit = "3")
        static boolean doCached(@SuppressWarnings("unused") AILType type, Object value,
                @Cached("type") AILType cachedType, @CachedLibrary("value") InteropLibrary valueLib) {
            return cachedType.isInstance.check(valueLib, value);
        }

        @TruffleBoundary
        @Specialization(replaces = "doCached")
        static boolean doGeneric(AILType type, Object value) {
            return type.isInstance.check(InteropLibrary.getFactory().getUncached(), value);
        }
    }

    /*
     * A convenience interface for type checks. Alternatively this could have been solved using
     * subtypes of SLType.
     */
    @FunctionalInterface
    interface TypeCheck {

        boolean check(InteropLibrary lib, Object value);

    }

}
