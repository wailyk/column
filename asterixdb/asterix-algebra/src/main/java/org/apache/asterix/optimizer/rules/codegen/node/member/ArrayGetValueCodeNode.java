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
package org.apache.asterix.optimizer.rules.codegen.node.member;

import org.apache.asterix.optimizer.rules.codegen.node.ICodeNodeVisitor;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class ArrayGetValueCodeNode extends AbstractMemberCodeNode {
    private static final long serialVersionUID = 857067402611036983L;
    private final String code;

    public ArrayGetValueCodeNode(IdentifierCodeNode arrayVar, IdentifierCodeNode indexVar) {
        super(arrayVar, indexVar);
        code = arrayVar + "[" + indexVar + "]";
    }

    @Override
    public String toString() {
        return code;
    }

    @Override
    public <R, T> R accept(ICodeNodeVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visit(this, arg);
    }
}
