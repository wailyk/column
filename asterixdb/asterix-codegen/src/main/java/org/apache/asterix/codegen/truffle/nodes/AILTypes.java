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
package org.apache.asterix.codegen.truffle.nodes;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.runtime.AILMissingRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILNull;
import org.apache.asterix.codegen.truffle.runtime.AILNullRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.codegen.truffle.runtime.aggregation.AILGeneralAggregator;
import org.apache.asterix.codegen.truffle.runtime.aggregation.AILMinMaxTopKAggregator;
import org.apache.asterix.codegen.truffle.runtime.reader.column.AILColumnReader;

import com.oracle.truffle.api.dsl.TypeCast;
import com.oracle.truffle.api.dsl.TypeCheck;
import com.oracle.truffle.api.dsl.TypeSystem;

/**
 * The type system of SL, as explained in {@link AILLanguage}. Based on the {@link TypeSystem}
 * annotation, the Truffle DSL generates the subclass {@link AILTypesGen} with type test and type
 * conversion methods for some types. In this class, we only cover types where the automatically
 * generated ones would not be sufficient.
 */
@TypeSystem({ long.class, boolean.class, double.class, AILStringRuntime.class, AILColumnReader.class,
        AILMinMaxTopKAggregator.class, AILGeneralAggregator.class })
public abstract class AILTypes {

    /**
     * Example of a manually specified type check that replaces the automatically generated type
     * check that the Truffle DSL would generate. For {@link AILNull}, we do not need an
     * {@code instanceof} check, because we know that there is only a {@link AILNull#SINGLETON
     * singleton} instance.
     */
    @TypeCheck(AILNull.class)
    public static boolean isAILNull(Object value) {
        return value == AILNull.SINGLETON;
    }

    /**
     * Example of a manually specified type cast that replaces the automatically generated type cast
     * that the Truffle DSL would generate. For {@link AILNull}, we do not need an actual cast,
     * because we know that there is only a {@link AILNull#SINGLETON singleton} instance.
     */
    @TypeCast(AILNull.class)
    public static AILNull asAILNull(Object value) {
        assert isAILNull(value);
        return AILNull.SINGLETON;
    }

    @TypeCheck(AILNullRuntime.class)
    public static boolean isNullRuntime(Object value) {
        return value == AILNullRuntime.INSTANCE;
    }

    @TypeCast(AILNullRuntime.class)
    public static AILNullRuntime asAILNullRuntime(Object value) {
        assert isNullRuntime(value);
        return AILNullRuntime.INSTANCE;
    }

    @TypeCheck(AILMissingRuntime.class)
    public static boolean isMissingRuntime(Object value) {
        return value == AILMissingRuntime.INSTANCE;
    }

    @TypeCast(AILMissingRuntime.class)
    public static AILMissingRuntime asAILMissingRuntime(Object value) {
        assert isMissingRuntime(value);
        return AILMissingRuntime.INSTANCE;
    }
}
