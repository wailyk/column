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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.codegen.node.BlockCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeGenTemplates;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeContext;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.BooleanLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.DoubleLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.LongLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.RuntimeStringLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.unary.NotOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.schema.CodeGenExpectedSchemaBuilder;
import org.apache.asterix.optimizer.rules.codegen.schema.NestedReaderBinderVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ScalarExpressionCodeGenVisitor implements ILogicalExpressionVisitor<ICodeNode, Set<IdentifierCodeNode>> {
    private static final String NOT_SCALAR_EXPRESSION = "Not a scalar expression";
    private final CodeGenExpectedSchemaBuilder schemaBuilder;
    private final NestedReaderBinderVisitor readerBinder;
    private final Set<IdentifierCodeNode> usedReaders;
    private final Set<LogicalVariable> removedListfy;
    private CodeNodeContext context;

    public ScalarExpressionCodeGenVisitor(CodeGenExpectedSchemaBuilder schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
        readerBinder = new NestedReaderBinderVisitor();
        removedListfy = new HashSet<>();
        usedReaders = new HashSet<>();
    }

    public void enterScope(CodeNodeContext context) {
        this.context = context;
        usedReaders.clear();
        removedListfy.clear();
    }

    public CodeNodeContext getContext() {
        return context;
    }

    public void addRemovedListfy(LogicalVariable aggLogicalVariable) {
        removedListfy.add(aggLogicalVariable);
    }

    public boolean isRemovedListify(LogicalVariable variable) {
        return removedListfy.contains(variable);
    }

    public List<ICodeNode> bindReader(LogicalVariable variable, ILogicalExpression expr) throws AlgebricksException {
        List<ICodeNode> readers;
        if (schemaBuilder.isVariableRegistered(variable)) {
            IExpectedSchemaNode node = schemaBuilder.getNodeFromVariable(variable);
            readers = readerBinder.bind(context, node, null, null);
        } else {
            //This is a nested expression and not a reader access
            readers = Collections.emptyList();
            ICodeNode commonExpression = toNode(expr);
            context.assign(variable, commonExpression, usedReaders);
        }
        return readers;
    }

    public List<ICodeNode> bindReaderForLoop(SourceLocation sourceLocation, LogicalVariable variable,
            BlockCodeNode loopBlock) {
        IExpectedSchemaNode node = schemaBuilder.getNodeFromVariable(variable);
        List<ICodeNode> loopNodes = readerBinder.bind(context, node, loopBlock, null);
        IdentifierCodeNode readerNode = readerBinder.getFirstReader();
        ICodeNode endOfArray = CodeGenTemplates.getIsEndOfArray(readerNode);
        ICodeNode loopCondition = new NotOperatorCodeNode(sourceLocation, endOfArray);
        loopNodes.add(0, loopCondition);
        return loopNodes;
    }

    public IdentifierCodeNode bindAndConvertToArray(LogicalVariable variable, IdentifierCodeNode indexVar,
            BlockCodeNode loopBlock) {
        IExpectedSchemaNode node = schemaBuilder.getNodeFromVariable(variable);
        List<ICodeNode> arrays = readerBinder.bind(context, node, loopBlock, indexVar);
        return (IdentifierCodeNode) arrays.get(0);
    }

    public ICodeNode toNode(ILogicalExpression expr) throws AlgebricksException {
        usedReaders.clear();
        return expr.accept(this, usedReaders);
    }

    public ICodeNode toNode(LogicalVariable variable) {
        usedReaders.clear();
        return context.getValue(variable, usedReaders);
    }

    public Set<IdentifierCodeNode> getUsedReaders() {
        return usedReaders;
    }

    @Override
    public ICodeNode visitVariableReferenceExpression(VariableReferenceExpression expr,
            Set<IdentifierCodeNode> usedReaders) {
        return context.getValue(expr.getVariableReference(), usedReaders);
    }

    @Override
    public ICodeNode visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr,
            Set<IdentifierCodeNode> usedReaders) throws AlgebricksException {
        IExpectedSchemaNode schemaNode = schemaBuilder.getNodeFromExpression(expr);
        if (schemaNode != null) {
            return context.getValue(schemaNode, usedReaders);
        }
        return ScalarFunctionsTemplatesUtil.toNode(expr, this);
    }

    @Override
    public ICodeNode visitConstantExpression(ConstantExpression expr, Set<IdentifierCodeNode> usedReaders)
            throws AlgebricksException {
        ATypeTag constTypeTag = ConstantExpressionUtil.getConstantIaObjectType(expr);
        SourceLocation location = expr.getSourceLocation();
        ICodeNode constNode;
        switch (constTypeTag) {
            case BOOLEAN:
                boolean bVal = ConstantExpressionUtil.getBooleanConstant(expr);
                constNode = new BooleanLiteralCodeNode(location, bVal);
                break;
            case BIGINT:
                long intVal = ConstantExpressionUtil.getLongConstant(expr);
                constNode = new LongLiteralCodeNode(location, intVal);
                break;
            case DOUBLE:
                double doubleVal = ConstantExpressionUtil.getDoubleConstant(expr);
                constNode = new DoubleLiteralCodeNode(location, doubleVal);
                break;
            case STRING:
                String stringVal = ConstantExpressionUtil.getStringConstant(expr);
                constNode = new RuntimeStringLiteralCodeNode(location, stringVal);
                break;
            default:
                constNode = null;
        }
        return constNode;
    }

    @Override
    public ICodeNode visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr,
            Set<IdentifierCodeNode> usedReaders) throws AlgebricksException {
        throw new IllegalStateException(NOT_SCALAR_EXPRESSION);
    }

    @Override
    public ICodeNode visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr,
            Set<IdentifierCodeNode> usedReaders) throws AlgebricksException {
        throw new IllegalStateException(NOT_SCALAR_EXPRESSION);
    }

    @Override
    public ICodeNode visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr,
            Set<IdentifierCodeNode> usedReaders) throws AlgebricksException {
        throw new IllegalStateException(NOT_SCALAR_EXPRESSION);
    }
}
