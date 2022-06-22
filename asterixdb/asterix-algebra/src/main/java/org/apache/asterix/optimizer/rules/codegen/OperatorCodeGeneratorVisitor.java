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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetFormat;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.optimizer.rules.codegen.node.BlockCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeContext;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.IfCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.visitor.StringCodeGeneratorVisitor;
import org.apache.asterix.optimizer.rules.codegen.schema.CodeGenExpectedSchemaBuilder;
import org.apache.asterix.optimizer.rules.codegen.schema.SchemaPathSplitterVisitor;
import org.apache.asterix.optimizer.rules.pushdown.OperatorValueAccessPushdownVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class OperatorCodeGeneratorVisitor implements ILogicalOperatorVisitor<Void, ILogicalOperator> {
    private final IOptimizationContext opContext;
    private final Set<ILogicalOperator> visitedOperators;
    private final CodeGenExpectedSchemaBuilder schemaBuilder;
    private final OperatorValueAccessPushdownVisitor schemaInferVisitor;
    private final ScalarExpressionCodeGenVisitor expressionVisitor;
    private final SchemaPathSplitterVisitor pathSplitter;
    private final Map<ILogicalOperator, CodeNodeContext> scopeContextMap;
    private int topK;
    private CodeNodeContext currentContext;
    private SelectOperator currentSelectOp;

    public OperatorCodeGeneratorVisitor(IOptimizationContext opContext) {
        this.opContext = opContext;
        visitedOperators = new HashSet<>();
        schemaBuilder = new CodeGenExpectedSchemaBuilder(opContext);
        schemaInferVisitor = new OperatorValueAccessPushdownVisitor(opContext, schemaBuilder);
        expressionVisitor = new ScalarExpressionCodeGenVisitor(schemaBuilder);
        pathSplitter = new SchemaPathSplitterVisitor();
        scopeContextMap = new HashMap<>();
    }

    public void getGeneratedCode(ILogicalOperator root) throws AlgebricksException {
        root.accept(schemaInferVisitor, null);
        root.accept(this, null);
        StringCodeGeneratorVisitor codeGenerator = new StringCodeGeneratorVisitor();
        for (CodeNodeContext context : scopeContextMap.values()) {
            context.finish(expressionVisitor, codeGenerator);
        }
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        DataSource dataSource = (DataSource) op.getDataSource();

        if (dataSource == null) {
            visitInputs(op);
            return null;
        }

        MetadataProvider mp = (MetadataProvider) opContext.getMetadataProvider();
        DataverseName dataverse = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasourceName();
        Dataset dataset = mp.findDataset(dataverse, datasetName);

        //Only external dataset can have pushed down expressions
        if (dataset.getDatasetType() == DatasetConfig.DatasetType.EXTERNAL
                || dataset.getDatasetFormatInfo().getFormat() == DatasetFormat.ROW) {
            visitInputs(op);
            return null;
        }

        String functionName = datasetName + scopeContextMap.size();
        LogicalVariable recordVariable = dataSource.getDataRecordVariable(op.getScanVariables());
        IExpectedSchemaNode root = schemaBuilder.getNodeFromVariable(recordVariable);
        CodeNodeContext context = new CodeNodeContext(functionName, op, schemaBuilder, pathSplitter.split(root),
                pathSplitter.getSourceInformationMap());
        scopeContextMap.put(op, context);
        currentContext = context;
        expressionVisitor.enterScope(context);
        return null;
    }

    /* *****************************
     * Visit supported operators
     * *****************************
     */

    @Override
    public Void visitSelectOperator(SelectOperator op, ILogicalOperator arg) throws AlgebricksException {
        currentSelectOp = op;
        visitInputs(op);
        if (currentContext == null) {
            return null;
        }

        ICodeNode condition = expressionVisitor.toNode(op.getCondition().getValue());
        BlockCodeNode block = currentContext.getCurrentBlock();
        //Else's block is always empty
        BlockCodeNode elseBlock = BlockCodeNode.EMPTY;
        //Make Then's block as the current block
        BlockCodeNode thenBlock = currentContext.createAndEnterBlock(op.getOperatorTag());
        block.appendNode(new IfCodeNode(condition, thenBlock, elseBlock));
        removeOp(op, arg);
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        if (currentContext == null) {
            return null;
        }
        List<LogicalVariable> variables = op.getVariables();

        if (currentContext.shouldProject(variables)) {
            currentContext.projectOutput(variables);
        }

        removeOp(op, arg);
        return null;
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        //Nested scope means in a subplan
        if (currentContext == null || op.isGlobal() && !currentContext.isNestedScope()) {
            return null;
        }
        //Non-grouped aggregations
        IVariableTypeEnvironment typeEnv = op.computeInputTypeEnvironment(opContext);
        List<Mutable<ILogicalExpression>> expressions = op.getExpressions();
        List<LogicalVariable> variables = op.getVariables();
        //Clear all output and only output aggregate variables
        currentContext.clearOutput();
        BlockCodeNode mainBlock = currentContext.getMain().getBlock();
        for (int i = 0; i < expressions.size(); i++) {
            AggregateFunctionCallExpression expr = (AggregateFunctionCallExpression) expressions.get(i).getValue();
            ICodeNode aggregateVar =
                    AggregateFunctionsTemplatesUtil.toNode(expr, expressionVisitor, currentContext, mainBlock);
            Object type = typeEnv.getType(expr);
            currentContext.putOutput(variables.get(i), aggregateVar, type);
        }
        currentContext.exitToMainBlock();

        removeOp(op, arg);
        //End of this scope
        currentContext = null;
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        if (currentContext == null) {
            return null;
        }

        IVariableTypeEnvironment typeEnv = op.computeInputTypeEnvironment(opContext);
        List<Mutable<ILogicalExpression>> expressions = op.getExpressions();
        List<LogicalVariable> variables = op.getVariables();

        for (int i = expressions.size() - 1; i >= 0; i--) {
            LogicalVariable variable = variables.remove(i);
            ILogicalExpression expression = expressions.remove(i).getValue();
            BlockCodeNode currentBlock = currentContext.getCurrentBlock();
            currentBlock.appendNodes(expressionVisitor.bindReader(variable, expression));
            currentContext.pushAssignToDataScan(variable, typeEnv.getType(expression));
        }

        removeOp(op, arg);
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        if (currentContext == null) {
            return null;
        }
        IVariableTypeEnvironment typeEnv = op.computeInputTypeEnvironment(opContext);
        UnnestUtils.handleUnnest(op, currentContext, typeEnv, expressionVisitor);
        removeOp(op, arg);
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, ILogicalOperator arg) throws AlgebricksException {
        if (op.isGlobal()) {
            topK = GroupByUtils.getTopK(op, arg);
        }
        visitInputs(op);
        if (op.isGlobal() || currentContext == null) {
            return null;
        }

        IVariableTypeEnvironment typeEnv = op.computeInputTypeEnvironment(opContext);

        GroupByUtils.handleUnnestGroupBy(typeEnv, opContext, op, expressionVisitor, currentContext);
        if (!GroupByUtils.handleMinMaxTopK(typeEnv, op, expressionVisitor, currentContext, topK)) {
            GroupByUtils.handleGeneralAggregate(typeEnv, op, expressionVisitor, currentContext, opContext);
        }
        if (op.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.ORDER) {
            //In case it is SORT_GROUP_BY, then remove the operator
            removeOp(op.getInputs().get(0).getValue(), op);
        }
        removeOp(op, arg);

        topK = -1;
        currentContext = null;
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        AggregateOperator aggOp = SubplanUtils.getRootAggregate(op);
        if (aggOp == null || currentContext == null) {
            currentContext = null;
            return null;
        }
        IVariableTypeEnvironment typeEnv = op.computeInputTypeEnvironment(opContext);
        if (SubplanUtils.isQuantified(aggOp)) {
            SubplanUtils.handleQuantifiedExpression(this, typeEnv, currentContext, aggOp);
            removeOp(op, arg);
            return null;
        } else if (SubplanUtils.isExists(aggOp, currentSelectOp)) {
            SubplanUtils.handleExists(this, typeEnv, currentContext, aggOp, currentSelectOp);
            removeOp(op, arg);
            return null;
        } else if (SubplanUtils.isListifyLimitMicroSort(aggOp)) {
            SubplanUtils.handleListifyLimitMicroSort(this, expressionVisitor, currentContext, aggOp);
            removeOp(op, arg);
            return null;
        } else if (SubplanUtils.isListifyAggregate(aggOp)) {
            SubplanUtils.handleListifyAggregate(this, expressionVisitor, currentContext, aggOp);
            removeOp(op, arg);
            return null;
        }
        currentContext = null;

        return null;
    }

    /* *****************************
     * Helpers
     * *****************************
     */

    public void visitInputs(ILogicalOperator op) throws AlgebricksException {
        if (visitedOperators.contains(op)) {
            return;
        }
        visitedOperators.add(op);

        for (Mutable<ILogicalOperator> opRef : op.getInputs()) {
            opRef.getValue().accept(this, op);
        }
    }

    //TODO add to a list for later removal
    private void removeOp(ILogicalOperator toRemove, ILogicalOperator parent) throws AlgebricksException {
        if (parent == null || currentContext.isNestedScope()) {
            return;
        }
        if (parent.getInputs().size() != 1 || toRemove.getInputs().size() != 1) {
            throw new AlgebricksException("Operator cannot be removed");
        }
        Mutable<ILogicalOperator> opRef = parent.getInputs().get(0);
        opRef.setValue(toRemove.getInputs().get(0).getValue());
        opContext.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
        opContext.computeAndSetTypeEnvironmentForOperator(parent);
    }

    /* *****************************
     * Visit unsupported operators
     * *****************************
     */

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, ILogicalOperator arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        if (arg.getOperatorTag() != LogicalOperatorTag.GROUP) {
            currentContext = null;
        }
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, ILogicalOperator arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, ILogicalOperator arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, ILogicalOperator arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;

    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        if (op.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.ONE_TO_ONE_EXCHANGE) {
            //New scope
            currentContext = null;
        }
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, ILogicalOperator arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;

    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, ILogicalOperator arg) throws AlgebricksException {
        visitInputs(op);
        currentContext = null;
        return null;
    }
}
