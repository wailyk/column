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
package org.apache.asterix.optimizer.rules.codegen;

import java.util.List;

import org.apache.asterix.optimizer.rules.codegen.node.BlockCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeGenTemplates;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeContext;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.WhileCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.AssignOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.AddScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.LTOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class UnnestUtils {
    private UnnestUtils() {
    }

    public static void handleUnnest(UnnestOperator op, CodeNodeContext context, IVariableTypeEnvironment typeEnv,
            ScalarExpressionCodeGenVisitor exprVisitor) throws AlgebricksException {
        if (op.getPositionalVariable() == null) {
            handleGenericUnnest(op, context, typeEnv, exprVisitor);
        } else {
            handleUnnestWithPositionalVariable(op, context, typeEnv, exprVisitor);
        }
    }

    private static void handleGenericUnnest(UnnestOperator op, CodeNodeContext context,
            IVariableTypeEnvironment typeEnv, ScalarExpressionCodeGenVisitor exprVisitor) throws AlgebricksException {
        SourceLocation sourceLocation = op.getSourceLocation();
        LogicalVariable unnestVariable = op.getVariable();
        BlockCodeNode currentBlock = context.getCurrentBlock();
        BlockCodeNode whileBlock = context.createBlock(op.getOperatorTag());
        List<ICodeNode> loopNodes = exprVisitor.bindReaderForLoop(sourceLocation, unnestVariable, whileBlock);
        ICodeNode condition = loopNodes.remove(0);
        ICodeNode whileNode = new WhileCodeNode(sourceLocation, condition, whileBlock);
        currentBlock.appendNode(whileNode);
        whileBlock.appendNodesToTail(loopNodes);

        context.enterBlock(whileBlock);
        context.pushAssignToDataScan(unnestVariable, typeEnv.getType(op.getExpressionRef().getValue()));
    }

    private static void handleUnnestWithPositionalVariable(UnnestOperator op, CodeNodeContext context,
            IVariableTypeEnvironment typeEnv, ScalarExpressionCodeGenVisitor exprVisitor) throws AlgebricksException {
        SourceLocation sourceLocation = op.getSourceLocation();
        LogicalVariable unnestVariable = op.getVariable();
        LogicalVariable positionalVariable = op.getPositionalVariable();
        BlockCodeNode currentBlock = context.getCurrentBlock();

        IdentifierCodeNode indexVar = currentBlock.declareVariable(context, CodeGenTemplates.ZERO);
        BlockCodeNode whileBlock = context.createBlock(op.getOperatorTag());
        IdentifierCodeNode firstArray = exprVisitor.bindAndConvertToArray(unnestVariable, indexVar, whileBlock);

        ICodeNode arrayCount = CodeGenTemplates.createArrayCount(firstArray);

        ICodeNode condition = new LTOperatorCodeNode(indexVar, arrayCount);
        ICodeNode whileNode = new WhileCodeNode(sourceLocation, condition, whileBlock);
        currentBlock.appendNode(whileNode);
        ICodeNode incNode = new AddScalarOperatorCodeNode(indexVar, CodeGenTemplates.ONE);
        whileBlock.appendNodeToTail(new AssignOperatorCodeNode(indexVar, incNode));

        context.enterBlock(whileBlock);
        context.bindPositionalVariable(unnestVariable, positionalVariable);
        context.assign(positionalVariable, indexVar, null);
    }
}
