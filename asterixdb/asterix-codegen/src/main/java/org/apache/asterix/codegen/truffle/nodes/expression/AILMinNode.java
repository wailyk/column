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

import org.apache.asterix.codegen.truffle.AILException;
import org.apache.asterix.codegen.truffle.nodes.AILBinaryNode;
import org.apache.asterix.codegen.truffle.runtime.AILMissingRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILNullRuntime;

import com.oracle.truffle.api.dsl.Fallback;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "min")
public abstract class AILMinNode extends AILBinaryNode {

    @Specialization
    public long min(long left, long right) {
        return Math.min(left, right);
    }

    @Specialization
    public long min(long left, AILNullRuntime right) {
        return left;
    }

    @Specialization
    public double min(double left, double right) {
        return Math.min(left, right);
    }

    @Specialization
    public long min(AILNullRuntime left, long right) {
        return right;
    }

    @Specialization
    public long min(long left, AILMissingRuntime right) {
        return left;
    }

    @Specialization
    public long min(AILMissingRuntime left, long right) {
        return right;
    }

    @Specialization
    public double min(double left, AILNullRuntime right) {
        return left;
    }

    @Specialization
    public double min(AILNullRuntime left, double right) {
        return right;
    }

    @Specialization
    public double min(double left, AILMissingRuntime right) {
        return left;
    }

    @Specialization
    public double min(AILMissingRuntime left, double right) {
        return right;
    }

    @Fallback
    protected Object typeError(Object left, Object right) {
        throw AILException.typeError(this, left, right);
    }
}
