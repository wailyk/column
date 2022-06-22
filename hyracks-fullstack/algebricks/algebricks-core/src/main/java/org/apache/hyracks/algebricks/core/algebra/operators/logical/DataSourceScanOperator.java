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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.projection.IProjectionInfo;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class DataSourceScanOperator extends AbstractDataSourceOperator {
    private final List<LogicalVariable> projectVars;
    private final List<Object> projectedVarsTypes;

    private List<Mutable<ILogicalExpression>> additionalFilteringExpressions;
    private List<LogicalVariable> minFilterVars;
    private List<LogicalVariable> maxFilterVars;

    // the select condition in the SELECT operator. Only results satisfying this selectCondition
    // would be returned by this operator
    private Mutable<ILogicalExpression> selectCondition;
    // the maximum of number of results output by this operator
    private long outputLimit = -1;

    private IProjectionInfo<?> datasetProjectionInfo;
    private IProjectionInfo<?> metaProjectionInfo;

    public DataSourceScanOperator(List<LogicalVariable> variables, IDataSource<?> dataSource) {
        this(variables, dataSource, null, -1, null, null);
    }

    public DataSourceScanOperator(List<LogicalVariable> variables, IDataSource<?> dataSource,
            Mutable<ILogicalExpression> selectCondition, long outputLimit, IProjectionInfo<?> datasetProjectionInfo,
            IProjectionInfo<?> metaProjectionInfo) {
        super(variables, dataSource);
        //Initially, all variables are projected
        projectVars = new ArrayList<>(variables);
        projectedVarsTypes = new ArrayList<>();
        //Set the types for the initial scan variables
        projectedVarsTypes.addAll(Arrays.asList(dataSource.getSchemaTypes()));
        this.selectCondition = selectCondition;
        this.outputLimit = outputLimit;
        this.datasetProjectionInfo = datasetProjectionInfo;
        this.metaProjectionInfo = metaProjectionInfo;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.DATASOURCESCAN;
    }

    @Override
    public <R, S> R accept(ILogicalOperatorVisitor<R, S> visitor, S arg) throws AlgebricksException {
        return visitor.visitDataScanOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return selectCondition != null && visitor.transform(selectCondition);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    public void projectVariable(LogicalVariable variable, Object type) {
        variables.add(variable);
        projectVars.add(variable);
        projectedVarsTypes.add(type);
    }

    public void projectOnly(List<LogicalVariable> projectedVariables) {
        int preProjectNumOfVars = dataSource.getSchemaTypes().length;

        for (int i = projectVars.size() - 1; i >= 0; i--) {
            if (!projectedVariables.contains(projectVars.get(i))) {
                if (i >= preProjectNumOfVars) {
                    //Do not remove PKs, record, meta variables
                    variables.remove(i);
                }
                projectedVarsTypes.remove(i);
                projectVars.remove(i);
            }
        }
    }

    public void clearProject() {
        projectVars.clear();
        projectedVarsTypes.clear();
    }

    public List<LogicalVariable> getProjectVariables() {
        return projectVars;
    }

    public boolean isProjectPushed() {
        return projectVars.size() < variables.size();
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                if (sources.length > 0) {
                    target.addAllVariables(sources[0]);
                }
                List<LogicalVariable> outputVariables = isProjectPushed() ? projectVars : variables;
                for (LogicalVariable v : outputVariables) {
                    target.addVariable(v);
                }
            }
        };
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);
        for (int i = 0; i < projectVars.size(); i++) {
            env.setVarType(projectVars.get(i), projectedVarsTypes.get(i));
        }
        return env;
    }

    public List<LogicalVariable> getMinFilterVars() {
        return minFilterVars;
    }

    public void setMinFilterVars(List<LogicalVariable> minFilterVars) {
        this.minFilterVars = minFilterVars;
    }

    public List<LogicalVariable> getMaxFilterVars() {
        return maxFilterVars;
    }

    public void setMaxFilterVars(List<LogicalVariable> maxFilterVars) {
        this.maxFilterVars = maxFilterVars;
    }

    public void setAdditionalFilteringExpressions(List<Mutable<ILogicalExpression>> additionalFilteringExpressions) {
        this.additionalFilteringExpressions = additionalFilteringExpressions;
    }

    public List<Mutable<ILogicalExpression>> getAdditionalFilteringExpressions() {
        return additionalFilteringExpressions;
    }

    public Mutable<ILogicalExpression> getSelectCondition() {
        return selectCondition;
    }

    public void setSelectCondition(Mutable<ILogicalExpression> selectCondition) {
        this.selectCondition = selectCondition;
    }

    public long getOutputLimit() {
        return outputLimit;
    }

    public void setOutputLimit(long outputLimit) {
        this.outputLimit = outputLimit;
    }

    public void setDatasetProjectionInfo(IProjectionInfo<?> projectionInfo) {
        this.datasetProjectionInfo = projectionInfo;
    }

    public IProjectionInfo<?> getDatasetProjectionInfo() {
        return datasetProjectionInfo;
    }

    public void setMetaProjectionInfo(IProjectionInfo<?> metaProjectionInfo) {
        this.metaProjectionInfo = metaProjectionInfo;
    }

    public IProjectionInfo<?> getMetaProjectionInfo() {
        return metaProjectionInfo;
    }
}
