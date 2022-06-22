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
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "++")
public abstract class AILAggregateAddNode extends AILBinaryNode {

    @Specialization(rewriteOn = ArithmeticException.class)
    protected long addLongs(long left, long right) {
        return Math.addExact(left, right);
    }

    @Specialization(replaces = "addLongs")
    protected long maxLong(long left, long right) {
        return Long.MAX_VALUE;
    }

    @Specialization
    protected double add(double left, double right) {
        return left + right;
    }

    @Specialization
    protected double add(double left, long right) {
        return left + right;
    }

    @Specialization
    protected double add(long left, double right) {
        return left + right;
    }

    @Specialization
    protected long add(long left, AILNullRuntime right) {
        return left;
    }

    @Specialization
    protected long add(long left, AILMissingRuntime right) {
        return left;
    }

    @Specialization
    protected long add(AILNullRuntime left, long right) {
        return right;
    }

    @Specialization
    protected long add(AILMissingRuntime left, long right) {
        return right;
    }

    @Specialization
    protected double add(double left, AILNullRuntime right) {
        return left;
    }

    @Specialization
    protected double add(double left, AILMissingRuntime right) {
        return left;
    }

    @Specialization
    protected double add(AILNullRuntime left, double right) {
        return right;
    }

    @Specialization
    protected double add(AILMissingRuntime left, double right) {
        return right;
    }

    @Specialization
    protected AILNullRuntime add(AILNullRuntime left, AILNullRuntime right) {
        return AILNullRuntime.INSTANCE;
    }
}
