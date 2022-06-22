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

import org.apache.asterix.codegen.truffle.nodes.AILExpressionNode;

import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Logical conjunction node with short circuit evaluation.
 */
@NodeInfo(shortName = "&&")
public final class AILLogicalAndNode extends AILShortCircuitNode {

    public AILLogicalAndNode(AILExpressionNode left, AILExpressionNode right) {
        super(left, right);
    }

    /**
     * The right value does not need to be evaluated if the left value is already <code>false</code>
     * .
     */
    @Override
    protected boolean isEvaluateRight(boolean left) {
        return left;
    }

    /**
     * Only if left and right value are true the result of the logical and is <code>true</code>. If
     * the second parameter is not evaluated, <code>false</code> is provided.
     */
    @Override
    protected boolean execute(boolean left, boolean right) {
        return left && right;
    }

}
