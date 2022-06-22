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

import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;

public class MethodCallCodeNode extends AbstractMemberCodeNode {
    private static final long serialVersionUID = 857067402611036983L;
    public static final ICodeNode[] EMPTY_ARGS = new ICodeNode[0];

    private final ICodeNode[] args;

    public MethodCallCodeNode(IdentifierCodeNode object, IdentifierCodeNode member) {
        this(object, member, EMPTY_ARGS);
    }

    public MethodCallCodeNode(IdentifierCodeNode object, IdentifierCodeNode member, ICodeNode... args) {
        super(object, member);
        this.args = args;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        buildMemberInfo(builder);
        builder.append('(');
        if (args != EMPTY_ARGS) {
            builder.append(args[0]);
            for (int i = 1; i < args.length; i++) {
                builder.append(", ");
                builder.append(args[i]);
            }
        }
        builder.append(')');
        return builder.toString();
    }
}
