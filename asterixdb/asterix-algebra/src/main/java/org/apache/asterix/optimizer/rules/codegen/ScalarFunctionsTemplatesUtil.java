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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.codegen.node.BlockCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeContext;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.IfCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.AssignOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.AddScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.DivideScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.ModScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.MultiplyScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.SubtractScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.AndOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.EqualsOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.GEOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.GTOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.LEOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.LTOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.NotEqualsOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.logical.OrOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.BuiltinFunctionProvider;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.NullLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.unary.NotOperatorCodeNode;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ScalarFunctionsTemplatesUtil {
    private ScalarFunctionsTemplatesUtil() {

    }

    public static ICodeNode toNode(ScalarFunctionCallExpression funcExpr, ScalarExpressionCodeGenVisitor exprVisitor)
            throws AlgebricksException {
        IFunctionInfo fInfo = funcExpr.getFunctionInfo();
        if (isUnaryOperator(fInfo)) {
            return handleUnaryOperator(funcExpr, exprVisitor);
        }
        if (isBinaryLogicalOperator(fInfo)) {
            return handleBinaryOperator(funcExpr, exprVisitor);
        } else if (isConjunctOrDisjunct(fInfo)) {
            return handleConjuntDisjunct(funcExpr, exprVisitor);
        } else if (isSwitchCase(fInfo)) {
            return handleSwitchCase(funcExpr, exprVisitor);
        } else if (isArrayFunction(fInfo)) {
            return handleArrayFunction(funcExpr, exprVisitor);
        } else {
            return handleBuiltinFunction(funcExpr, exprVisitor);
        }

    }

    private static boolean isUnaryOperator(IFunctionInfo fInfo) {
        return fInfo.getFunctionIdentifier() == BuiltinFunctions.NOT;
    }

    private static boolean isBinaryLogicalOperator(IFunctionInfo fInfo) {
        FunctionIdentifier fId = fInfo.getFunctionIdentifier();
        return fId == BuiltinFunctions.EQ || fId == BuiltinFunctions.GT || fId == BuiltinFunctions.GE
                || fId == BuiltinFunctions.LT || fId == BuiltinFunctions.LE || fId == BuiltinFunctions.NEQ
                || fId.getName().contains("numeric");
    }

    private static boolean isConjunctOrDisjunct(IFunctionInfo fInfo) {
        FunctionIdentifier fId = fInfo.getFunctionIdentifier();
        return fId == BuiltinFunctions.AND || fId == BuiltinFunctions.OR;
    }

    private static boolean isSwitchCase(IFunctionInfo fInfo) {
        FunctionIdentifier fId = fInfo.getFunctionIdentifier();
        return fId == BuiltinFunctions.SWITCH_CASE;
    }

    private static boolean isArrayFunction(IFunctionInfo fInfo) {
        FunctionIdentifier fId = fInfo.getFunctionIdentifier();
        return fId == BuiltinFunctions.GET_ITEM;
    }

    private static ICodeNode handleUnaryOperator(ScalarFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor) throws AlgebricksException {
        ILogicalExpression arg = funcExpr.getArguments().get(0).getValue();
        ICodeNode boolNode = arg.accept(exprVisitor, exprVisitor.getUsedReaders());
        return new NotOperatorCodeNode(funcExpr.getSourceLocation(), boolNode);
    }

    private static ICodeNode handleBinaryOperator(AbstractFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor) throws AlgebricksException {
        ILogicalExpression left = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression right = funcExpr.getArguments().get(1).getValue();

        ICodeNode leftNode = left.accept(exprVisitor, exprVisitor.getUsedReaders());
        ICodeNode rightNode = right.accept(exprVisitor, exprVisitor.getUsedReaders());
        return createBinaryOperator(funcExpr, leftNode, rightNode);
    }

    private static ICodeNode handleConjuntDisjunct(ScalarFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        ICodeNode left = args.get(0).getValue().accept(exprVisitor, exprVisitor.getUsedReaders());
        for (int i = 1; i < args.size(); i++) {
            ICodeNode right = args.get(i).getValue().accept(exprVisitor, exprVisitor.getUsedReaders());
            left = createBinaryOperator(funcExpr, left, right);
        }
        return left;
    }

    private static ICodeNode handleSwitchCase(ScalarFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor) throws AlgebricksException {
        CodeNodeContext context = exprVisitor.getContext();
        BlockCodeNode currentBlock = context.getCurrentBlock();
        IdentifierCodeNode caseVar = currentBlock.declareVariable(context, NullLiteralCodeNode.NULL);
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        toIfElse(exprVisitor, context, caseVar, args, 1, currentBlock);
        return caseVar;
    }

    private static void toIfElse(ScalarExpressionCodeGenVisitor exprVisitor, CodeNodeContext context,
            IdentifierCodeNode caseVar, List<Mutable<ILogicalExpression>> args, int index, BlockCodeNode blockNode)
            throws AlgebricksException {
        if (args.size() == index + 1) {
            ILogicalExpression expr = args.get(index).getValue();
            ICodeNode exprValue = expr.accept(exprVisitor, exprVisitor.getUsedReaders());
            blockNode.appendNode(new AssignOperatorCodeNode(caseVar, exprValue));
            return;
        }
        BlockCodeNode elseBlock = context.createBlock(LogicalOperatorTag.SELECT);
        toIfElse(exprVisitor, context, caseVar, args, index + 2, elseBlock);

        BlockCodeNode thenBlock = context.createBlock(LogicalOperatorTag.SELECT);
        ILogicalExpression conditionExpr = args.get(index).getValue();
        ICodeNode conditionValue = conditionExpr.accept(exprVisitor, exprVisitor.getUsedReaders());
        ILogicalExpression thenExpr = args.get(index + 1).getValue();
        ICodeNode thenValue = thenExpr.accept(exprVisitor, exprVisitor.getUsedReaders());
        thenBlock.appendNode(new AssignOperatorCodeNode(caseVar, thenValue));

        blockNode.appendNode(new IfCodeNode(conditionValue, thenBlock, elseBlock));
    }

    private static ICodeNode handleArrayFunction(ScalarFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor) throws AlgebricksException {
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        ILogicalExpression arg = funcExpr.getArguments().get(0).getValue();
        if (arg.getExpressionTag() != LogicalExpressionTag.VARIABLE || fid != BuiltinFunctions.GET_ITEM) {
            throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, funcExpr.getSourceLocation(), fid.getName());
        }
        LogicalVariable variable = VariableUtilities.getVariable(arg);
        if (!exprVisitor.isRemovedListify(variable)) {
            throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, funcExpr.getSourceLocation(), fid.getName());
        }
        return exprVisitor.toNode(arg);
    }

    private static ICodeNode handleBuiltinFunction(ScalarFunctionCallExpression funcExpr,
            ScalarExpressionCodeGenVisitor exprVisitor) throws AlgebricksException {
        List<ICodeNode> args = new ArrayList<>();
        for (Mutable<ILogicalExpression> exprRef : funcExpr.getArguments()) {
            args.add(exprRef.getValue().accept(exprVisitor, exprVisitor.getUsedReaders()));
        }

        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        SourceLocation sourceLocation = funcExpr.getSourceLocation();
        ICodeNode[] argsArray = args.toArray(new ICodeNode[0]);
        return BuiltinFunctionProvider.createFunction(fid, sourceLocation, argsArray);
    }

    protected static ICodeNode createBinaryOperator(AbstractFunctionCallExpression funcExpr, ICodeNode left,
            ICodeNode right) {
        FunctionIdentifier fId = funcExpr.getFunctionIdentifier();
        SourceLocation location = funcExpr.getSourceLocation();
        if (fId == BuiltinFunctions.EQ) {
            return new EqualsOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.GT) {
            return new GTOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.GE) {
            return new GEOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.LT) {
            return new LTOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.LE) {
            return new LEOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.AND) {
            return new AndOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.OR) {
            return new OrOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.NEQ) {
            return new NotEqualsOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.NUMERIC_SUBTRACT) {
            return new SubtractScalarOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.NUMERIC_ADD) {
            return new AddScalarOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.NUMERIC_MULTIPLY) {
            return new MultiplyScalarOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.NUMERIC_DIVIDE) {
            return new DivideScalarOperatorCodeNode(location, left, right);
        } else if (fId == BuiltinFunctions.NUMERIC_MOD) {
            return new ModScalarOperatorCodeNode(location, left, right);
        }
        return null;
    }

    protected static ICodeNode createNestedConjunct(SourceLocation sourceLocation, List<ICodeNode> booleans,
            int index) {
        if (index + 1 == booleans.size()) {
            return booleans.get(index);
        }
        ICodeNode left = booleans.get(index);
        ICodeNode right = createNestedConjunct(sourceLocation, booleans, index + 1);
        return new AndOperatorCodeNode(sourceLocation, left, right);
    }
}
