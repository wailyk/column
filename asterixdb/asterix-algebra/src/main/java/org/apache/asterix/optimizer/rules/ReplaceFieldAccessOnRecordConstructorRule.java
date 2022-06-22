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
package org.apache.asterix.optimizer.rules;

import java.util.List;
import java.util.Map;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ReplaceFieldAccessOnRecordConstructorRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            return rewrite(op, context);
        }
        return false;
    }

    private boolean rewrite(ILogicalOperator op, IOptimizationContext context) {
        AssignOperator assignOp = (AssignOperator) op;
        Map<LogicalVariable, EquivalenceClass> map = context.getEquivalenceClassMap(op);
        List<Mutable<ILogicalExpression>> exprList = assignOp.getExpressions();
        return rewriteExprList(map, exprList);
    }

    private boolean rewriteExprList(Map<LogicalVariable, EquivalenceClass> map,
            List<Mutable<ILogicalExpression>> exprList) {
        boolean changed = false;
        for (Mutable<ILogicalExpression> expr : exprList) {
            changed |= rewriteExpr(map, expr);
        }
        return changed;
    }

    private boolean rewriteExpr(Map<LogicalVariable, EquivalenceClass> map, Mutable<ILogicalExpression> arg) {
        ILogicalExpression expr = arg.getValue();
        LogicalVariable variable = getFirstArgAsVariable(map, arg.getValue());
        if (variable != null) {
            arg.setValue(new VariableReferenceExpression(variable));
            return true;
        } else if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return rewriteExprList(map, ((AbstractFunctionCallExpression) expr).getArguments());
        }
        return false;
    }

    private LogicalVariable getFirstArgAsVariable(Map<LogicalVariable, EquivalenceClass> map,
            ILogicalExpression expression) {
        if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
            for (EquivalenceClass eqClass : map.values()) {
                if (eqClass.getExpressionMembers().contains(expression)) {
                    return eqClass.getVariableRepresentative();
                }
            }
        }
        return null;
    }

}
