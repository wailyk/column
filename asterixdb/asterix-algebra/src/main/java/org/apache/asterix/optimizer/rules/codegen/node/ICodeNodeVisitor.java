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
package org.apache.asterix.optimizer.rules.codegen.node;

import org.apache.asterix.optimizer.rules.codegen.node.control.BreakCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.IfCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.ReturnCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.WhileCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.AbstractBinaryCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.AbstractBuiltinFunction;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.AbstractLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.unary.AbstractUnaryNode;
import org.apache.asterix.optimizer.rules.codegen.node.member.AbstractMemberCodeNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public interface ICodeNodeVisitor<R, T> {
    R visit(AbstractLiteralCodeNode node, T arg) throws AlgebricksException;

    R visit(AbstractUnaryNode node, T arg) throws AlgebricksException;

    R visit(AbstractBinaryCodeNode node, T arg) throws AlgebricksException;

    R visit(AbstractBuiltinFunction node, T arg) throws AlgebricksException;

    R visit(IdentifierCodeNode node, T arg) throws AlgebricksException;

    R visit(IfCodeNode node, T arg) throws AlgebricksException;

    R visit(WhileCodeNode node, T arg) throws AlgebricksException;

    R visit(ReturnCodeNode node, T arg) throws AlgebricksException;

    R visit(BreakCodeNode node, T arg) throws AlgebricksException;

    R visit(AbstractMemberCodeNode methodCallCodeNode, T arg) throws AlgebricksException;

    R visit(MainFunctionCodeNode mainFunctionCodeNode, T arg) throws AlgebricksException;

    R visit(BlockCodeNode blockCodeNode, T arg) throws AlgebricksException;
}
