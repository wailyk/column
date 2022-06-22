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
package org.apache.asterix.codegen.truffle.nodes.expression;

import org.apache.asterix.codegen.truffle.nodes.AILBinaryNode;
import org.apache.asterix.codegen.truffle.runtime.AILMissingRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILNullRuntime;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "+")
public abstract class AILAddNode extends AILBinaryNode {

    @Specialization
    protected long add(long left, long right) {
        return left + right;
    }

    @Specialization
    protected double add(double left, double right) {
        return left + right;
    }

    @Specialization(limit = "4")
    protected Object doGeneric(Object left, Object right, @CachedLibrary("left") InteropLibrary leftLib,
            @CachedLibrary("right") InteropLibrary rightLib) {
        if (left == AILMissingRuntime.INSTANCE || right == AILMissingRuntime.INSTANCE) {
            return AILMissingRuntime.INSTANCE;
        } else if (left == AILNullRuntime.INSTANCE || right == AILNullRuntime.INSTANCE) {
            return AILNullRuntime.INSTANCE;
        } else {
            //Unsupported type
            return AILMissingRuntime.INSTANCE;
        }
    }
}
