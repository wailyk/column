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
package org.apache.asterix.optimizer.rules.codegen.schema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.optimizer.rules.pushdown.ExpectedSchemaBuilder;
import org.apache.asterix.optimizer.rules.pushdown.schema.AbstractComplexExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class CodeGenExpectedSchemaBuilder extends ExpectedSchemaBuilder {
    private final Map<AbstractFunctionCallExpression, LogicalVariable> exprToVar;
    private final Map<LogicalVariable, AbstractFunctionCallExpression> varToExpr;
    private final Map<LogicalVariable, LogicalVariable> unnestVarToPosVar;
    private final Set<LogicalVariable> usedVariables;
    private final IOptimizationContext context;

    public CodeGenExpectedSchemaBuilder(IOptimizationContext context) {
        this.context = context;
        exprToVar = new HashMap<>();
        varToExpr = new HashMap<>();
        unnestVarToPosVar = new HashMap<>();
        usedVariables = new HashSet<>();
    }

    @Override
    public void unregisterVariable(LogicalVariable variable) {
        IExpectedSchemaNode node = varToNode.get(variable);
        if (node.getType() != ExpectedSchemaNodeType.ANY) {
            ((AbstractComplexExpectedSchemaNode) node).setRequestedEntirely();
        }
    }

    @Override
    protected void putExpr(AbstractFunctionCallExpression expr, IExpectedSchemaNode leaf) {
        LogicalVariable dummyVar = context.newVar();
        putVar(expr, dummyVar, leaf);
    }

    @Override
    protected void putVar(AbstractFunctionCallExpression expr, LogicalVariable variable, IExpectedSchemaNode node) {
        if (!exprToVar.containsKey(expr)) {
            exprToVar.put(expr, variable);
            varToNode.put(variable, node);
        } else {
            LogicalVariable commonVar = exprToVar.get(expr);
            IExpectedSchemaNode commonNode = varToNode.get(commonVar);
            varToNode.put(variable, commonNode);
        }
        varToExpr.put(variable, expr);
    }

    public void setPositionalVariable(LogicalVariable unnestVariable, LogicalVariable positionalVariable) {
        unnestVarToPosVar.put(unnestVariable, positionalVariable);
    }

    public LogicalVariable getPositionalVariableIfAny(LogicalVariable variable) {
        ILogicalExpression expr = varToExpr.get(variable);
        usedVariables.clear();
        expr.getUsedVariables(usedVariables);
        for (LogicalVariable usedVar : usedVariables) {
            if (unnestVarToPosVar.containsKey(usedVar)) {
                return unnestVarToPosVar.get(usedVar);
            }
        }
        for (LogicalVariable usedVar : usedVariables) {
            LogicalVariable posVar = getPositionalVariableIfAny(usedVar);
            if (posVar != null) {
                return posVar;
            }
        }
        return null;
    }

    public IExpectedSchemaNode getNodeFromVariable(LogicalVariable variable) {
        return varToNode.get(variable);
    }

    public IExpectedSchemaNode getNodeFromExpression(AbstractFunctionCallExpression expression) {
        return varToNode.get(exprToVar.get(expression));
    }
}
