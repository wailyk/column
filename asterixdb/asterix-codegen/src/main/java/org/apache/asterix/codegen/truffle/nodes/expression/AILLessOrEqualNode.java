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
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * This class is similar to the {@link AILLessThanNode}.
 */
@NodeInfo(shortName = "<=")
public abstract class AILLessOrEqualNode extends AILBinaryNode {

    @Specialization
    protected boolean lessOrEqual(long left, long right) {
        return left <= right;
    }

    @Specialization
    protected boolean lessOrEqual(double left, double right) {
        return left <= right;
    }

    @Specialization
    protected boolean lessOrEqual(long left, double right) {
        return left <= right;
    }

    @Specialization
    protected boolean lessOrEqual(double left, long right) {
        return left <= right;
    }

    @Specialization
    protected boolean lessOrEqual(AILStringRuntime left, AILStringRuntime right) {
        return left.compare(right) <= 0;
    }

    @Specialization
    protected boolean lessOrEqual(long left, AILStringRuntime right) {
        return false;
    }

    @Specialization
    protected boolean lessOrEqual(AILStringRuntime left, long right) {
        return false;
    }

    @Specialization
    protected boolean lessOrEqual(double left, AILStringRuntime right) {
        return false;
    }

    @Specialization
    protected boolean lessOrEqual(AILStringRuntime left, double right) {
        return false;
    }

    @Specialization
    protected boolean lessOrEqual(AILStringRuntime left, AILNullRuntime right) {
        return false;
    }

    @Specialization
    protected boolean lessOrEqual(AILNullRuntime left, AILStringRuntime right) {
        return false;
    }

    @Specialization
    protected boolean lessOrEqual(AILStringRuntime left, AILMissingRuntime right) {
        return false;
    }

    @Specialization
    protected boolean lessOrEqual(AILMissingRuntime left, AILStringRuntime right) {
        return false;
    }

}
