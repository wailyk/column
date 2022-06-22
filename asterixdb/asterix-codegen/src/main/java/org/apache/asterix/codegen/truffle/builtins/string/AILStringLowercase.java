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
package org.apache.asterix.codegen.truffle.builtins.string;

import java.io.IOException;

import org.apache.asterix.codegen.truffle.AILRuntimeException;
import org.apache.asterix.codegen.truffle.builtins.AILBuiltinNode;
import org.apache.asterix.codegen.truffle.runtime.AILMissingRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILNullRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "lowercase")
public abstract class AILStringLowercase extends AILBuiltinNode {
    private final UTF8StringBuilder stringBuilder;
    private final GrowableArray lowercaseStorage;
    private final AILStringRuntime lowercase;
    private final UTF8StringPointable stringPointable;

    AILStringLowercase() {
        stringBuilder = new UTF8StringBuilder();
        lowercaseStorage = new GrowableArray();
        lowercase = new AILStringRuntime();
        stringPointable = new UTF8StringPointable();
    }

    @Specialization
    @TruffleBoundary
    public AILStringRuntime lowercase(AILStringRuntime value) {
        lowercaseStorage.reset();
        try {
            stringPointable.set(value.getStringValue());
            stringPointable.lowercase(stringBuilder, lowercaseStorage);
        } catch (IOException e) {
            throw new AILRuntimeException();
        }
        lowercase.reset(lowercaseStorage);
        return lowercase;
    }

    @Specialization
    public Object lowercase(AILMissingRuntime value) {
        return value;
    }

    @Specialization
    public Object lowercase(AILNullRuntime value) {
        return value;
    }
}
