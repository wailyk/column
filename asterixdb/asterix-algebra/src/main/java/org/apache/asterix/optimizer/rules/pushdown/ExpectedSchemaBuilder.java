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
package org.apache.asterix.optimizer.rules.pushdown;

import static org.apache.asterix.optimizer.rules.pushdown.ExpressionValueAccessPushdownVisitor.ARRAY_FUNCTIONS;
import static org.apache.asterix.optimizer.rules.pushdown.ExpressionValueAccessPushdownVisitor.SUPPORTED_FUNCTIONS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.pushdown.schema.AbstractComplexExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ArrayExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ObjectExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.UnionExpectedSchemaNode;
import org.apache.asterix.runtime.projection.DataProjectionInfo;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

/**
 * This class takes a value access expression and produces an expected schema (given the expression).
 * Example:
 * - $$t.getField("hashtags").getItem(0)
 * We expect:
 * 1- $$t is OBJECT
 * 2- the output type of getField("hashtags") is ARRAY
 * 3- the output type of getItem(0) is ANY node
 */
public class ExpectedSchemaBuilder {
    //Registered Variables
    protected final Map<LogicalVariable, IExpectedSchemaNode> varToNode;
    protected final ExpectedSchemaNodeToIATypeTranslatorVisitor typeBuilder;

    public ExpectedSchemaBuilder() {
        varToNode = new HashMap<>();
        typeBuilder = new ExpectedSchemaNodeToIATypeTranslatorVisitor();
    }

    public DataProjectionInfo createProjectionInfo(LogicalVariable recordVariable) {
        IExpectedSchemaNode rootNode = varToNode.get(recordVariable);
        Map<String, FunctionCallInformation> sourceInformation = new HashMap<>();
        typeBuilder.reset(sourceInformation);
        ARecordType recordType = (ARecordType) rootNode.accept(typeBuilder, null);
        return new DataProjectionInfo(recordType, sourceInformation);
    }

    public boolean setSchemaFromExpression(AbstractFunctionCallExpression expr, LogicalVariable producedVar,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        //Parent always nested
        AbstractComplexExpectedSchemaNode parent = (AbstractComplexExpectedSchemaNode) buildNestedNode(expr, typeEnv);
        if (parent != null) {
            IExpectedSchemaNode leaf =
                    new AnyExpectedSchemaNode(parent, expr.getSourceLocation(), expr.getFunctionIdentifier());
            leaf = addChild(expr, typeEnv, parent, leaf);
            if (producedVar != null) {
                //Register the node if a variable is produced
                putVar(expr, producedVar, leaf);
            } else {
                putExpr(expr, leaf);
            }
        }
        return parent != null;
    }

    protected void putExpr(AbstractFunctionCallExpression expr, IExpectedSchemaNode leaf) {
        //NoOp
    }

    public void registerRoot(LogicalVariable recordVar, RootExpectedSchemaNode rootNode) {
        varToNode.put(recordVar, rootNode);
    }

    public void unregisterVariable(LogicalVariable variable) {
        //Remove the node so no other expression will pushdown any expression in the future
        IExpectedSchemaNode node = varToNode.remove(variable);
        AbstractComplexExpectedSchemaNode parent = node.getParent();
        if (parent == null) {
            //It is a root node. Request the entire record
            varToNode.put(variable, RootExpectedSchemaNode.ALL_FIELDS_ROOT_NODE);
        } else {
            //It is a nested node. Replace the node to a LEAF node
            node.replaceIfNeeded(ExpectedSchemaNodeType.ANY, parent.getSourceLocation(),
                    parent.getFunctionIdentifier());
        }
    }

    public boolean isVariableRegistered(LogicalVariable recordVar) {
        return varToNode.containsKey(recordVar);
    }

    public boolean containsRegisteredDatasets() {
        return !varToNode.isEmpty();
    }

    protected IExpectedSchemaNode buildNestedNode(ILogicalExpression expr, IVariableTypeEnvironment typeEnv)
            throws AlgebricksException {
        //The current node expression
        AbstractFunctionCallExpression myExpr = (AbstractFunctionCallExpression) expr;
        if (!SUPPORTED_FUNCTIONS.contains(myExpr.getFunctionIdentifier()) || noArgsOrFirstArgIsConstant(myExpr)) {
            //Return null if the function is not supported.
            return null;
        }

        //The parent expression
        ILogicalExpression parentExpr = myExpr.getArguments().get(0).getValue();
        if (isVariable(parentExpr)) {
            //A variable could be the record's originated from data-scan or an expression from assign
            LogicalVariable sourceVar = VariableUtilities.getVariable(parentExpr);
            return changeNodeForVariable(sourceVar, myExpr);
        }

        //Recursively create the parent nodes. Parent is always a nested node
        AbstractComplexExpectedSchemaNode newParent =
                (AbstractComplexExpectedSchemaNode) buildNestedNode(parentExpr, typeEnv);
        //newParent could be null if the expression is not supported
        if (newParent != null) {
            //Parent expression must be a function call (as parent is a nested node)
            AbstractFunctionCallExpression parentFuncExpr = (AbstractFunctionCallExpression) parentExpr;
            //Get 'myType' as we will create the child type of the newParent
            ExpectedSchemaNodeType myType = getExpectedNestedNodeType(myExpr);
            /*
             * Create 'myNode'. It is a nested node because the function is either getField() or a supported array
             * function
             */
            AbstractComplexExpectedSchemaNode myNode = AbstractComplexExpectedSchemaNode.createNestedNode(myType,
                    newParent, myExpr.getSourceLocation(), myExpr.getFunctionIdentifier());
            //Add myNode to the parent
            addChild(parentFuncExpr, typeEnv, newParent, myNode);
            return myNode;
        }
        return null;
    }

    private boolean noArgsOrFirstArgIsConstant(AbstractFunctionCallExpression myExpr) {
        List<Mutable<ILogicalExpression>> args = myExpr.getArguments();
        return args.isEmpty() || args.get(0).getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT;
    }

    private IExpectedSchemaNode changeNodeForVariable(LogicalVariable sourceVar,
            AbstractFunctionCallExpression myExpr) {
        //Get the associated node with the sourceVar (if any)
        IExpectedSchemaNode oldNode = varToNode.get(sourceVar);
        if (oldNode == null) {
            //Variable is not associated with a node. No pushdown is possible
            return null;
        }
        return replaceNode(sourceVar, myExpr, oldNode);
    }

    protected IExpectedSchemaNode replaceNode(LogicalVariable sourceVar, AbstractFunctionCallExpression myExpr,
            IExpectedSchemaNode oldNode) {
        //What is the expected type of the variable
        ExpectedSchemaNodeType varExpectedType = getExpectedNestedNodeType(myExpr);
        // Get the node associated with the variable (or change its type if needed).
        IExpectedSchemaNode newNode =
                oldNode.replaceIfNeeded(varExpectedType, myExpr.getSourceLocation(), myExpr.getFunctionIdentifier());
        //Map the sourceVar to the node
        varToNode.put(sourceVar, newNode);
        return newNode;
    }

    protected IExpectedSchemaNode addChild(AbstractFunctionCallExpression parentExpr, IVariableTypeEnvironment typeEnv,
            AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child) throws AlgebricksException {
        switch (parent.getType()) {
            case OBJECT:
                return handleObject(parentExpr, typeEnv, parent, child);

            case ARRAY:
                return handleArray(parent, child);
            case UNION:
                return handleUnion(parentExpr, parent, child);
            default:
                throw new IllegalStateException("Node " + parent.getType() + " is not nested");
        }
    }

    protected void putVar(AbstractFunctionCallExpression expr, LogicalVariable variable, IExpectedSchemaNode node) {
        varToNode.put(variable, node);
    }

    private IExpectedSchemaNode handleObject(AbstractFunctionCallExpression parentExpr,
            IVariableTypeEnvironment typeEnv, AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child)
            throws AlgebricksException {
        if (BuiltinFunctions.FIELD_ACCESS_BY_NAME.equals(parentExpr.getFunctionIdentifier())) {
            ObjectExpectedSchemaNode objectNode = (ObjectExpectedSchemaNode) parent;
            return objectNode.addChild(ConstantExpressionUtil.getStringArgument(parentExpr, 1), child);
        } else {
            //FIELD_ACCESS_BY_INDEX
            ARecordType recordType = (ARecordType) typeEnv.getType(parentExpr.getArguments().get(0).getValue());
            ObjectExpectedSchemaNode objectNode = (ObjectExpectedSchemaNode) parent;
            int fieldIdx = ConstantExpressionUtil.getIntArgument(parentExpr, 1);
            return objectNode.addChild(recordType.getFieldNames()[fieldIdx], child);
        }
    }

    private IExpectedSchemaNode handleArray(AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child) {
        ArrayExpectedSchemaNode arrayNode = (ArrayExpectedSchemaNode) parent;
        return arrayNode.addChild(child);
    }

    private IExpectedSchemaNode handleUnion(AbstractFunctionCallExpression parentExpr,
            AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child) throws AlgebricksException {
        UnionExpectedSchemaNode unionNode = (UnionExpectedSchemaNode) parent;
        ExpectedSchemaNodeType parentType = getExpectedNestedNodeType(parentExpr);
        return addChild(parentExpr, null, unionNode.getChild(parentType), child);
    }

    private static ExpectedSchemaNodeType getExpectedNestedNodeType(AbstractFunctionCallExpression funcExpr) {
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        if (BuiltinFunctions.FIELD_ACCESS_BY_NAME.equals(fid) || BuiltinFunctions.FIELD_ACCESS_BY_INDEX.equals(fid)) {
            return ExpectedSchemaNodeType.OBJECT;
        } else if (ARRAY_FUNCTIONS.contains(fid)) {
            return ExpectedSchemaNodeType.ARRAY;
        }
        throw new IllegalStateException("Function " + fid + " should not be pushed down");
    }

    private static boolean isVariable(ILogicalExpression expr) {
        return expr.getExpressionTag() == LogicalExpressionTag.VARIABLE;
    }

    void assignVarToVar(LogicalVariable producedVariable, LogicalVariable variable) {
        if (producedVariable != null) {
            varToNode.put(producedVariable, varToNode.get(variable));
        }
    }
}
