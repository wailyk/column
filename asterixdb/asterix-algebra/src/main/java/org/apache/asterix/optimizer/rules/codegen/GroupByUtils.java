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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.codegen.node.BlockCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeContext;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.IfCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.AssignOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.aggregate.AggregateBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.aggregate.NewAggregatorBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.aggregate.NewTopKBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.FalseLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.LongLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.StringLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.TrueLiteralCodeNode;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

public class GroupByUtils {
    public static int getTopK(GroupByOperator groupByOp, ILogicalOperator parent) {
        int topK = -1;
        if (groupByOp.isGlobal() && groupByOp.getGroupByVarList().size() == 1
                && parent.getOperatorTag() == LogicalOperatorTag.ORDER) {
            OrderOperator orderOp = (OrderOperator) parent;

            AggregateOperator aggregateOp = getAggregatesOperator(groupByOp);
            Set<LogicalVariable> orderVars = getOrderVariables(orderOp.getOrderExpressions());
            if (aggregateOp != null && isMinMaxTopK(aggregateOp, orderVars)) {
                topK = orderOp.getTopK();
            }
        }
        return topK;
    }

    public static boolean handleMinMaxTopK(IVariableTypeEnvironment typeEnv, GroupByOperator op,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context, int k) throws AlgebricksException {
        AggregateOperator aggregateOp = GroupByUtils.getAggregatesOperator(op);
        if (k < 0 || aggregateOp == null) {
            return false;
        }
        ICodeNode topK = new LongLiteralCodeNode(k);
        AbstractFunctionCallExpression aggFunc =
                (AbstractFunctionCallExpression) aggregateOp.getExpressions().get(0).getValue();
        ICodeNode isMin = aggFunc.getFunctionIdentifier() == BuiltinFunctions.LOCAL_SQL_MIN ? TrueLiteralCodeNode.TRUE
                : FalseLiteralCodeNode.FALSE;
        ICodeNode newTopK = new NewTopKBuiltinCodeNode(aggregateOp.getSourceLocation(), topK, isMin);
        aggregate(typeEnv, op, aggregateOp, exprVisitor, context, newTopK);
        return true;
    }

    public static boolean handleUnnestGroupBy(IVariableTypeEnvironment typeEnv, IOptimizationContext opContext,
            GroupByOperator op, ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context)
            throws AlgebricksException {
        BlockCodeNode unnestBlock = getUnnestBlock(context);
        if (unnestBlock == null) {
            //No unnest
            return false;
        }

        List<LogicalVariable> groupByVars = op.getGroupByVarList();
        if (isGroupByVariableOriginatedFromUnnest(context, groupByVars, unnestBlock)) {
            /*
             * This means that both the group-by variables are bounded to the unnest block, which we cannot do
             * anything about it.
             *
             * For example:
             * SELECT dependentName, AVG(d.age)
             * FROM Employee e, e.dependents d
             * GROUP BY d.name AS dependentName
             *
             * In this case, the group-by variable `d.name` originated from the UNNEST
             */
            return false;
        }

        AggregateOperator aggregateOp = getAggregatesOperator(op);
        if (aggregateOp == null) {
            return false;
        }

        BlockCodeNode parentBlock = unnestBlock.getParent();
        BlockCodeNode currentBlock = context.getCurrentBlock();
        IdentifierCodeNode shouldIncludeResult =
                parentBlock.declareVariableAsFirstLine(context, FalseLiteralCodeNode.FALSE);
        currentBlock.appendNode(new AssignOperatorCodeNode(shouldIncludeResult, TrueLiteralCodeNode.TRUE));
        context.projectOutput(op.getGroupByVarList());
        List<Mutable<ILogicalExpression>> aggExprList = aggregateOp.getExpressions();
        for (Mutable<ILogicalExpression> exprRef : aggExprList) {
            AggregateFunctionCallExpression expr = (AggregateFunctionCallExpression) exprRef.getValue();
            extractAggArgs(typeEnv, opContext, context, exprVisitor, expr, parentBlock);
            changeAggFuncInfoIfNeeded(expr);
        }
        BlockCodeNode shouldIncludeResultBlock = context.createAndEnterBlock(LogicalOperatorTag.SELECT);
        BlockCodeNode elseBlock = BlockCodeNode.EMPTY;
        parentBlock.appendNode(new IfCodeNode(shouldIncludeResult, shouldIncludeResultBlock, elseBlock));
        return true;
    }

    public static void handleGeneralAggregate(IVariableTypeEnvironment typeEnv, GroupByOperator op,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context, IOptimizationContext opContext)
            throws AlgebricksException {
        AggregateOperator aggregateOp = GroupByUtils.getAggregatesOperator(op);
        if (aggregateOp == null) {
            return;
        }

        AbstractFunctionCallExpression funcExpr =
                (AbstractFunctionCallExpression) aggregateOp.getExpressions().get(0).getValue();
        FunctionIdentifier fId = funcExpr.getFunctionIdentifier();
        String type = null;
        if (fId == BuiltinFunctions.SQL_COUNT || fId == BuiltinFunctions.SERIAL_SQL_COUNT) {
            type = "COUNT";
        } else if (fId == BuiltinFunctions.LOCAL_SQL_SUM || fId == BuiltinFunctions.SERIAL_LOCAL_SQL_SUM) {
            type = "SUM";
        }

        ICodeNode aggType = new StringLiteralCodeNode(type);
        ICodeNode budget = getMemoryBudget(opContext);
        ICodeNode aggregator = new NewAggregatorBuiltinCodeNode(aggregateOp.getSourceLocation(), aggType, budget);
        aggregate(typeEnv, op, aggregateOp, exprVisitor, context, aggregator);
    }

    private static ICodeNode getMemoryBudget(IOptimizationContext opContext) {
        PhysicalOptimizationConfig config = opContext.getPhysicalOptimizationConfig();
        return new LongLiteralCodeNode((long) config.getMaxFramesForGroupBy() * config.getFrameSize());
    }

    private static void aggregate(IVariableTypeEnvironment typeEnv, GroupByOperator op, AggregateOperator aggregateOp,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context, ICodeNode aggregator)
            throws AlgebricksException {
        context.clearOutput();
        for (Mutable<ILogicalExpression> exprRef : aggregateOp.getExpressions()) {
            AggregateFunctionCallExpression aggExpr = (AggregateFunctionCallExpression) exprRef.getValue();
            LogicalVariable aggVar = aggregateOp.getVariables().get(0);

            //TODO support multiple keys
            Pair<LogicalVariable, Mutable<ILogicalExpression>> pair = op.getGroupByList().get(0);
            ILogicalExpression expression = pair.getSecond().getValue();
            LogicalVariable aggVariable = VariableUtilities.getVariable(expression);
            LogicalVariable variable = pair.getFirst();
            ICodeNode topKComputer = toNode(aggVariable, aggExpr, exprVisitor, context, aggregator);
            context.putOutput(variable, topKComputer, typeEnv.getType(expression));
            context.putOutput(aggVar, topKComputer, typeEnv.getType(aggExpr));
        }
        context.exitToMainBlock();
    }

    private static void changeAggFuncInfoIfNeeded(AggregateFunctionCallExpression expr) {
        FunctionIdentifier fId = expr.getFunctionIdentifier();
        if (fId == BuiltinFunctions.SQL_COUNT || fId == BuiltinFunctions.SERIAL_SQL_COUNT) {
            expr.setFunctionInfo(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.LOCAL_SQL_SUM));
        }
    }

    private static void extractAggArgs(IVariableTypeEnvironment typeEnv, IOptimizationContext opContext,
            CodeNodeContext context, ScalarExpressionCodeGenVisitor exprVisitor, AggregateFunctionCallExpression expr,
            BlockCodeNode parentBlock) throws AlgebricksException {
        Mutable<ILogicalExpression> argRef = expr.getArguments().get(0);
        ILogicalExpression argExpr = argRef.getValue();
        LogicalVariable argVar = VariableUtilities.getVariable(argExpr);
        if (argVar == null && argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            argVar = opContext.newVar();
            ICodeNode node = exprVisitor.toNode(argExpr);
            context.assign(argVar, node, exprVisitor.getUsedReaders());
            argRef.setValue(new VariableReferenceExpression(argVar));
        }
        ICodeNode aggregateVar = AggregateFunctionsTemplatesUtil.toNode(expr, exprVisitor, context, parentBlock);
        if (argVar != null) {
            context.putOutput(argVar, aggregateVar, typeEnv.getType(argExpr));
        }
    }

    private static boolean isGroupByVariableOriginatedFromUnnest(CodeNodeContext context,
            List<LogicalVariable> groupByVars, BlockCodeNode unnestBlock) {
        for (LogicalVariable variable : groupByVars) {
            if (context.isReaderBoundedToBlock(variable, unnestBlock)) {
                return true;
            }
        }
        return false;
    }

    private static BlockCodeNode getUnnestBlock(CodeNodeContext context) {
        BlockCodeNode parentBlock = context.getCurrentBlock();
        while (parentBlock.getTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (parentBlock.getTag() == LogicalOperatorTag.UNNEST) {
                return parentBlock;
            }
            parentBlock = parentBlock.getParent();
        }
        return null;
    }

    private static AggregateOperator getAggregatesOperator(GroupByOperator groupByOp) {
        List<ILogicalPlan> nestedPlans = groupByOp.getNestedPlans();
        if (nestedPlans.size() > 1 || nestedPlans.get(0).getRoots().size() > 1) {
            return null;
        }
        ILogicalOperator op = nestedPlans.get(0).getRoots().get(0).getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return null;
        }
        return (AggregateOperator) op;
    }

    private static ICodeNode toNode(LogicalVariable variable, AggregateFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context, ICodeNode aggregator)
            throws AlgebricksException {
        ILogicalExpression arg = funcExpr.getArguments().get(0).getValue();
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        if (fid == BuiltinFunctions.LOCAL_SQL_MAX || fid == BuiltinFunctions.LOCAL_SQL_MIN
                || fid == BuiltinFunctions.SQL_COUNT || fid == BuiltinFunctions.SERIAL_SQL_COUNT
                || fid == BuiltinFunctions.LOCAL_SQL_SUM || fid == BuiltinFunctions.SERIAL_LOCAL_SQL_SUM) {
            return handleAggregate(variable, arg, exprVisitor, context, aggregator);
        }
        return null;
    }

    private static ICodeNode handleAggregate(LogicalVariable groupByVar, ILogicalExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context, ICodeNode aggregator)
            throws AlgebricksException {
        BlockCodeNode mainBlock = context.getMain().getBlock();
        IdentifierCodeNode aggregatorVar = mainBlock.declareVariableAsFirstLine(context, aggregator);

        ICodeNode groupByVarName = exprVisitor.toNode(groupByVar);
        ICodeNode varName = funcExpr.accept(exprVisitor, exprVisitor.getUsedReaders());
        ICodeNode aggregate =
                new AggregateBuiltinCodeNode(funcExpr.getSourceLocation(), aggregatorVar, groupByVarName, varName);
        context.getCurrentBlock().appendNode(aggregate);
        return aggregatorVar;
    }

    private static boolean isMinMaxTopK(AggregateOperator aggregateOp, Set<LogicalVariable> orderVars) {
        List<Mutable<ILogicalExpression>> aggregateExpressions = aggregateOp.getExpressions();
        List<LogicalVariable> aggregateVariables = aggregateOp.getVariables();
        for (int i = 0; i < aggregateExpressions.size(); i++) {
            LogicalVariable aggVar = aggregateVariables.get(i);
            ILogicalExpression expression = aggregateExpressions.get(i).getValue();
            if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return false;
            }
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
            FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
            if (fid != BuiltinFunctions.GLOBAL_SQL_MAX && fid != BuiltinFunctions.GLOBAL_SQL_MIN) {
                return false;
            }
            orderVars.remove(aggVar);
        }
        //If empty, then it means the order is on aggregate values only --> which means we can compute it
        return orderVars.isEmpty();
    }

    private static Set<LogicalVariable> getOrderVariables(List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExpr) {
        Set<LogicalVariable> orderVars = new HashSet<>();
        for (Pair<IOrder, Mutable<ILogicalExpression>> pair : orderExpr) {
            LogicalVariable variable = VariableUtilities.getVariable(pair.second.getValue());
            if (variable == null) {
                return Collections.emptySet();
            }
            orderVars.add(variable);
        }
        return orderVars;
    }

}
