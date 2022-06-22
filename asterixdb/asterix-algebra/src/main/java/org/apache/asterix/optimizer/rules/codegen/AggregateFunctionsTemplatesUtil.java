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

import static org.apache.asterix.optimizer.rules.codegen.node.AbstractCodeNode.GENERATED_LOCATION;
import static org.apache.asterix.optimizer.rules.codegen.node.CodeGenTemplates.ONE;
import static org.apache.asterix.optimizer.rules.codegen.node.CodeGenTemplates.ZERO;
import static org.apache.asterix.optimizer.rules.codegen.node.expression.literal.NullLiteralCodeNode.NULL;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.codegen.node.BlockCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeContext;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.AssignOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.AddAggregateOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.MaxOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.MinOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.OneZeroBuiltinFunctionCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class AggregateFunctionsTemplatesUtil {

    private AggregateFunctionsTemplatesUtil() {
    }

    public static ICodeNode toNode(AggregateFunctionCallExpression funcExpr, ScalarExpressionCodeGenVisitor exprVisitor,
            CodeNodeContext context, BlockCodeNode aggregateDeclarationBlock) throws AlgebricksException {
        FunctionIdentifier fId = funcExpr.getFunctionInfo().getFunctionIdentifier();
        ICodeNode retNode = null;
        if (fId == BuiltinFunctions.SQL_COUNT || fId == BuiltinFunctions.SERIAL_SQL_COUNT) {
            retNode = handleCount(funcExpr, context, exprVisitor, aggregateDeclarationBlock);
        } else if (fId == BuiltinFunctions.LOCAL_SQL_MAX) {
            retNode = handleMax(funcExpr, exprVisitor, context, aggregateDeclarationBlock);
        } else if (fId == BuiltinFunctions.LOCAL_SQL_MIN) {
            retNode = handleMin(funcExpr, exprVisitor, context, aggregateDeclarationBlock);
        } else if (fId == BuiltinFunctions.LOCAL_SQL_SUM || fId == BuiltinFunctions.SERIAL_LOCAL_SQL_SUM
                || fId == BuiltinFunctions.SQL_SUM) {
            retNode = handleSum(funcExpr, exprVisitor, context, aggregateDeclarationBlock);
        }

        return retNode;
    }

    private static ICodeNode handleCount(AggregateFunctionCallExpression funcExpr, CodeNodeContext context,
            ScalarExpressionCodeGenVisitor exprVisitor, BlockCodeNode aggregateDeclarationBlock)
            throws AlgebricksException {
        IdentifierCodeNode countVar = aggregateDeclarationBlock.declareVariableAsFirstLine(context, ZERO);
        ILogicalExpression countArg = funcExpr.getArguments().get(0).getValue();
        AddAggregateOperatorCodeNode addOp;
        if (countArg.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            // count(*)
            addOp = new AddAggregateOperatorCodeNode(funcExpr.getSourceLocation(), countVar, ONE);
        } else {
            // count($$var)
            ICodeNode varName = countArg.accept(exprVisitor, exprVisitor.getUsedReaders());
            //Returns 0 if the value is UNKNOWN, 1 otherwise
            OneZeroBuiltinFunctionCodeNode oneZeroOp = new OneZeroBuiltinFunctionCodeNode(GENERATED_LOCATION, varName);
            addOp = new AddAggregateOperatorCodeNode(funcExpr.getSourceLocation(), countVar, oneZeroOp);
        }

        context.getCurrentBlock().appendNode(new AssignOperatorCodeNode(countVar, addOp));
        return countVar;
    }

    private static ICodeNode handleMax(AggregateFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context,
            BlockCodeNode aggregateDeclarationBlock) throws AlgebricksException {
        IdentifierCodeNode maxVar = aggregateDeclarationBlock.declareVariableAsFirstLine(context, NULL);
        ICodeNode varName = funcExpr.getArguments().get(0).getValue().accept(exprVisitor, exprVisitor.getUsedReaders());
        MaxOperatorCodeNode maxOp = new MaxOperatorCodeNode(funcExpr.getSourceLocation(), maxVar, varName);
        context.getCurrentBlock().appendNode(new AssignOperatorCodeNode(maxVar, maxOp));
        return maxVar;
    }

    private static ICodeNode handleMin(AggregateFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context,
            BlockCodeNode aggregateDeclarationBlock) throws AlgebricksException {
        IdentifierCodeNode minVar = aggregateDeclarationBlock.declareVariableAsFirstLine(context, NULL);
        ICodeNode varName = funcExpr.getArguments().get(0).getValue().accept(exprVisitor, exprVisitor.getUsedReaders());
        MinOperatorCodeNode maxOp = new MinOperatorCodeNode(funcExpr.getSourceLocation(), minVar, varName);
        context.getCurrentBlock().appendNode(new AssignOperatorCodeNode(minVar, maxOp));
        return minVar;
    }

    private static ICodeNode handleSum(AggregateFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context,
            BlockCodeNode aggregateDeclarationBlock) throws AlgebricksException {
        ICodeNode initValue = funcExpr.getFunctionIdentifier() == BuiltinFunctions.SQL_SUM ? NULL : ZERO;
        IdentifierCodeNode sumVar = aggregateDeclarationBlock.declareVariableAsFirstLine(context, initValue);
        ICodeNode varName = funcExpr.getArguments().get(0).getValue().accept(exprVisitor, exprVisitor.getUsedReaders());
        AddAggregateOperatorCodeNode addOp =
                new AddAggregateOperatorCodeNode(funcExpr.getSourceLocation(), sumVar, varName);
        context.getCurrentBlock().appendNode(new AssignOperatorCodeNode(sumVar, addOp));
        return sumVar;
    }

}
