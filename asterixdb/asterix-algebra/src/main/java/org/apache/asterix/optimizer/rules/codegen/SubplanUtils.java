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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.codegen.node.BlockCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeContext;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.BreakCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.IfCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.AssignOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.GTOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.LTOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.OrOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.IsUnknownBuiltinFunctionCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.FalseLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.NullLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.TrueLiteralCodeNode;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

public class SubplanUtils {
    private SubplanUtils() {
    }

    public static AggregateOperator getRootAggregate(SubplanOperator op) {
        if (op.getNumberOfRoots() != 1) {
            return null;
        }
        ILogicalOperator root = op.getNestedPlans().get(0).getRoots().get(0).getValue();
        if (root.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return null;
        }
        return (AggregateOperator) root;
    }

    /* *********************************************************************************************************
     * Pattern checking
     * *********************************************************************************************************
     */

    public static boolean isQuantified(AggregateOperator aggOp) {
        //TODO fix for EVERY
        AggregateFunctionCallExpression aggFunc =
                (AggregateFunctionCallExpression) aggOp.getExpressions().get(0).getValue();
        return aggOp.getExpressions().size() == 1
                && aggFunc.getFunctionIdentifier() == BuiltinFunctions.NON_EMPTY_STREAM;
    }

    public static boolean isExists(AggregateOperator aggOp, SelectOperator selectOp) {
        AggregateFunctionCallExpression aggFunc =
                (AggregateFunctionCallExpression) aggOp.getExpressions().get(0).getValue();
        return aggOp.getExpressions().size() == 1 && aggFunc.getFunctionIdentifier() == BuiltinFunctions.COUNT
                && aggFunc.getArguments().get(0).getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT
                && isNotEq(aggOp.getVariables().get(0), selectOp);
    }

    public static boolean isListifyLimitMicroSort(AggregateOperator aggOp) {
        if (aggOp.getExpressions().size() != 1) {
            return false;
        }
        ILogicalOperator op1 = aggOp.getInputs().get(0).getValue();
        ILogicalOperator op2 = op1.getInputs().get(0).getValue();
        if (!isLimitOne(op1) || !isMicroSortWithSingleExpression(op2)) {
            return false;
        }
        AggregateFunctionCallExpression aggFunc =
                (AggregateFunctionCallExpression) aggOp.getExpressions().get(0).getValue();
        return aggFunc.getFunctionIdentifier() == BuiltinFunctions.LISTIFY;
    }

    public static boolean isListifyAggregate(AggregateOperator aggOp) {
        if (aggOp.getExpressions().size() != 1) {
            return false;
        }
        ILogicalOperator op = aggOp.getInputs().get(0).getValue();
        if (!isAggregate(op)) {
            return false;
        }
        AggregateFunctionCallExpression aggFunc =
                (AggregateFunctionCallExpression) aggOp.getExpressions().get(0).getValue();
        return aggFunc.getFunctionIdentifier() == BuiltinFunctions.LISTIFY;
    }

    /* *********************************************************************************************************
     * Code Generation
     * *********************************************************************************************************
     */

    public static void handleQuantifiedExpression(OperatorCodeGeneratorVisitor opVisitor,
            IVariableTypeEnvironment typeEnv, CodeNodeContext context, AggregateOperator aggOp)
            throws AlgebricksException {
        BlockCodeNode outerBlock = context.getCurrentBlock();
        IdentifierCodeNode someVar = outerBlock.declareVariable(context, FalseLiteralCodeNode.FALSE);

        Map<LogicalVariable, IdentifierCodeNode> scope = context.enterNestedScope();
        opVisitor.visitInputs(aggOp);
        context.exitNestedScope(scope);

        BlockCodeNode innerBlock = context.getCurrentBlock();
        innerBlock.appendNode(new AssignOperatorCodeNode(someVar, TrueLiteralCodeNode.TRUE));
        innerBlock.appendNode(BreakCodeNode.INSTANCE);
        context.enterBlock(outerBlock);

        LogicalVariable someLogicalVar = aggOp.getVariables().get(0);
        ILogicalExpression someLogicalExpr = aggOp.getExpressions().get(0).getValue();
        context.assign(someLogicalVar, someVar, Collections.emptySet());
        context.pushAssignToDataScan(someLogicalVar, typeEnv.getType(someLogicalExpr));
    }

    public static void handleListifyLimitMicroSort(OperatorCodeGeneratorVisitor opVisitor,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context, AggregateOperator aggOp)
            throws AlgebricksException {
        BlockCodeNode outerBlock = context.getCurrentBlock();
        IdentifierCodeNode orderVar = outerBlock.declareVariable(context, NullLiteralCodeNode.NULL);
        IdentifierCodeNode listifyVar = outerBlock.declareVariable(context, NullLiteralCodeNode.NULL);

        LogicalVariable listifyLogicalVar = getListifyVariable(aggOp.getExpressions().get(0).getValue());

        OrderOperator orderOp = (OrderOperator) aggOp.getInputs().get(0).getValue().getInputs().get(0).getValue();
        Pair<IOrder, Mutable<ILogicalExpression>> orderPair = orderOp.getOrderExpressions().get(0);
        boolean isMin = orderPair.getFirst().getKind() == OrderKind.ASC;
        ILogicalExpression orderExpr = orderPair.getSecond().getValue();

        Map<LogicalVariable, IdentifierCodeNode> scope = context.enterNestedScope();
        opVisitor.visitInputs(orderOp);
        context.exitNestedScope(scope);

        ICodeNode orderValNode = exprVisitor.toNode(orderExpr);
        ICodeNode minMaxValNode =
                isMin ? new GTOperatorCodeNode(orderVar, orderValNode) : new LTOperatorCodeNode(orderVar, orderValNode);
        ICodeNode replaceCondition =
                new OrOperatorCodeNode(new IsUnknownBuiltinFunctionCodeNode(orderVar), minMaxValNode);
        ICodeNode listifyValNode = exprVisitor.toNode(listifyLogicalVar);

        BlockCodeNode innerBlock = context.getCurrentBlock();
        BlockCodeNode thenBlock = context.createAndEnterBlock(LogicalOperatorTag.SELECT);
        thenBlock.appendNode(new AssignOperatorCodeNode(orderVar, orderValNode));
        thenBlock.appendNode(new AssignOperatorCodeNode(listifyVar, listifyValNode));
        innerBlock.appendNode(new IfCodeNode(replaceCondition, thenBlock));

        LogicalVariable aggLogicalVar = aggOp.getVariables().get(0);
        exprVisitor.addRemovedListfy(aggLogicalVar);
        context.assign(aggLogicalVar, listifyVar, Collections.emptySet());
        context.pushAssignToDataScan(aggLogicalVar, BuiltinType.ANY);
        context.enterBlock(outerBlock);
    }

    public static void handleListifyAggregate(OperatorCodeGeneratorVisitor opVisitor,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context, AggregateOperator aggOp)
            throws AlgebricksException {
        BlockCodeNode outerBlock = context.getCurrentBlock();
        AggregateOperator innerAggOp = (AggregateOperator) aggOp.getInputs().get(0).getValue();
        AggregateFunctionCallExpression innerAggFunc =
                (AggregateFunctionCallExpression) innerAggOp.getExpressions().get(0).getValue();

        Map<LogicalVariable, IdentifierCodeNode> scope = context.enterNestedScope();
        opVisitor.visitInputs(innerAggOp);
        context.exitNestedScope(scope);

        ICodeNode innerAggVal = AggregateFunctionsTemplatesUtil.toNode(innerAggFunc, exprVisitor, context, outerBlock);

        LogicalVariable aggLogicalVar = aggOp.getVariables().get(0);
        exprVisitor.addRemovedListfy(aggLogicalVar);
        context.assign(aggLogicalVar, innerAggVal, Collections.emptySet());
        context.pushAssignToDataScan(aggLogicalVar, BuiltinType.ANY);
        context.enterBlock(outerBlock);
    }

    public static void handleExists(OperatorCodeGeneratorVisitor opVisitor, IVariableTypeEnvironment typeEnv,
            CodeNodeContext context, AggregateOperator aggOp, SelectOperator selectOp) throws AlgebricksException {
        BlockCodeNode outerBlock = context.getCurrentBlock();
        IdentifierCodeNode someVar = outerBlock.declareVariable(context, FalseLiteralCodeNode.FALSE);

        Map<LogicalVariable, IdentifierCodeNode> scope = context.enterNestedScope();
        opVisitor.visitInputs(aggOp);
        context.exitNestedScope(scope);

        BlockCodeNode innerBlock = context.getCurrentBlock();
        innerBlock.appendNode(new AssignOperatorCodeNode(someVar, TrueLiteralCodeNode.TRUE));
        context.enterBlock(outerBlock);

        LogicalVariable someLogicalVar = aggOp.getVariables().get(0);
        ILogicalExpression someLogicalExpr = aggOp.getExpressions().get(0).getValue();
        context.assign(someLogicalVar, someVar, Collections.emptySet());
        selectOp.getCondition().setValue(new VariableReferenceExpression(someLogicalVar));
        context.pushAssignToDataScan(someLogicalVar, typeEnv.getType(someLogicalExpr));
    }

    /* *********************************************************************************************************
     * Helper methods
     * *********************************************************************************************************
     */

    private static LogicalVariable getListifyVariable(ILogicalExpression listifyExpr) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) listifyExpr;
        return VariableUtilities.getVariable(funcExpr.getArguments().get(0).getValue());
    }

    private static boolean isAggregate(ILogicalOperator op) {
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggOp = (AggregateOperator) op;
        AbstractFunctionCallExpression aggExpr =
                (AbstractFunctionCallExpression) aggOp.getExpressions().get(0).getValue();
        return aggOp.getExpressions().size() == 1 && aggExpr.getFunctionIdentifier() != BuiltinFunctions.LISTIFY;
    }

    private static boolean isLimitOne(ILogicalOperator op) {
        if (op.getOperatorTag() != LogicalOperatorTag.LIMIT) {
            return false;
        }
        LimitOperator limitOp = (LimitOperator) op;
        Integer limit = ConstantExpressionUtil.getIntConstant(limitOp.getMaxObjects().getValue());
        return limit != null && limit == 1;
    }

    private static boolean isMicroSortWithSingleExpression(ILogicalOperator op) {
        if (op.getOperatorTag() != LogicalOperatorTag.ORDER) {
            return false;
        }
        OrderOperator orderOp = (OrderOperator) op;

        return orderOp.getOrderExpressions().size() == 1
                && orderOp.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.MICRO_STABLE_SORT;
    }

    private static boolean isNotEq(LogicalVariable variable, SelectOperator selectOp) {
        if (selectOp == null) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) selectOp.getCondition().getValue();
        if (funcExpr.getFunctionIdentifier() != BuiltinFunctions.NEQ) {
            return false;
        }
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        LogicalVariable argVar1 = VariableUtilities.getVariable(args.get(0).getValue());
        LogicalVariable argVar2 = VariableUtilities.getVariable(args.get(1).getValue());
        return variable.equals(argVar1) || variable.equals(argVar2);
    }

}
