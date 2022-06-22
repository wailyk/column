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
package org.apache.asterix.codegen.truffle.nodes.util;

import static com.oracle.truffle.api.CompilerDirectives.shouldNotReachHere;

import org.apache.asterix.codegen.truffle.nodes.AILExpressionNode;
import org.apache.asterix.codegen.truffle.nodes.AILTypes;
import org.apache.asterix.codegen.truffle.runtime.AILFunction;
import org.apache.asterix.codegen.truffle.runtime.AILMissingRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILNullRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.codegen.truffle.runtime.aggregation.AILGeneralAggregator;
import org.apache.asterix.codegen.truffle.runtime.aggregation.AILMinMaxTopKAggregator;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;

/**
 * The node to normalize any value to an SL value. This is useful to reduce the number of values
 * expression nodes need to expect.
 */
@TypeSystemReference(AILTypes.class)
@NodeChild
public abstract class AILUnboxNode extends AILExpressionNode {

    static final int LIMIT = 5;

    @Specialization
    protected static String fromString(String value) {
        return value;
    }

    @Specialization
    protected static boolean fromBoolean(boolean value) {
        return value;
    }

    @Specialization
    protected static double fromDouble(double value) {
        return value;
    }

    @Specialization
    protected static long fromLong(long value) {
        return value;
    }

    @Specialization
    protected static AILStringRuntime fromRuntimeString(AILStringRuntime value) {
        return value;
    }

    @Specialization
    protected static AILFunction fromFunction(AILFunction value) {
        return value;
    }

    @Specialization
    protected static AILMinMaxTopKAggregator fromTopKComputer(AILMinMaxTopKAggregator value) {
        return value;
    }

    @Specialization
    protected static AILGeneralAggregator fromStringHeapAggregator(AILGeneralAggregator value) {
        return value;
    }

    @Specialization
    protected static AILResultWriter fromResultWriter(AILResultWriter value) {
        return value;
    }

    @Specialization
    protected static AILNullRuntime fromRuntimeNull(AILNullRuntime value) {
        return value;
    }

    @Specialization
    protected static AILMissingRuntime fromRuntimeMissing(AILMissingRuntime value) {
        return value;
    }

    @Specialization(limit = "LIMIT")
    public static Object fromForeign(Object value, @CachedLibrary("value") InteropLibrary interop) {
        try {
            if (interop.fitsInLong(value)) {
                return interop.asLong(value);
            } else if (interop.fitsInDouble(value)) {
                return interop.asDouble(value);
            } else if (interop.isString(value)) {
                return interop.asString(value);
            } else if (interop.isBoolean(value)) {
                return interop.asBoolean(value);
            } else {
                return value;
            }
        } catch (UnsupportedMessageException e) {
            throw shouldNotReachHere(e);
        }
    }

}
