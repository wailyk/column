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
package org.apache.asterix.optimizer.rules.codegen.node.expression.unary;

import org.apache.asterix.optimizer.rules.codegen.node.AbstractCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeType;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class AbstractUnaryNode extends AbstractCodeNode {
    private static final long serialVersionUID = 7866803252772030639L;
    private final ICodeNode valueNode;

    AbstractUnaryNode(SourceLocation sourceLocation, ICodeNode valueNode) {
        super(sourceLocation);
        this.valueNode = valueNode;
    }

    public ICodeNode getValueNode() {
        return valueNode;
    }

    @Override
    public CodeNodeType getType() {
        return CodeNodeType.EXPRESSION;
    }

    public abstract String getOperator();
}
