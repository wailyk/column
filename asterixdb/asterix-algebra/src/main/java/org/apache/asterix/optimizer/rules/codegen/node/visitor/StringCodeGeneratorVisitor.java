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
package org.apache.asterix.optimizer.rules.codegen.node.visitor;

import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.visitor.SimpleStringBuilderForIATypeVisitor;
import org.apache.asterix.optimizer.rules.codegen.node.BlockCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeGenTemplates;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeType;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNodeVisitor;
import org.apache.asterix.optimizer.rules.codegen.node.MainFunctionCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.BreakCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.IfCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.ReturnCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.control.WhileCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.AbstractBinaryCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.AddScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.DivideScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.ModScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.MultiplyScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.arithmatic.SubtractScalarOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.AbstractBuiltinFunction;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.AbstractLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.unary.AbstractUnaryNode;
import org.apache.asterix.optimizer.rules.codegen.node.member.AbstractMemberCodeNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class StringCodeGeneratorVisitor implements ICodeNodeVisitor<Void, Integer> {
    private static final String INDENT = "   ";
    private final StringBuilder builder = new StringBuilder();
    private final SimpleStringBuilderForIATypeVisitor typePrinter = new SimpleStringBuilderForIATypeVisitor();

    public String generateCode(MainFunctionCodeNode main) throws AlgebricksException {
        builder.setLength(0);
        main.accept(this, 0);
        return builder.toString();
    }

    @Override
    public Void visit(AbstractLiteralCodeNode node, Integer arg) throws AlgebricksException {
        builder.append(node.toString());
        return null;
    }

    @Override
    public Void visit(AbstractUnaryNode node, Integer arg) throws AlgebricksException {
        builder.append(node.getOperator());
        node.getValueNode().accept(this, 0);
        return null;
    }

    @Override
    public Void visit(AbstractBinaryCodeNode node, Integer arg) throws AlgebricksException {
        indent(arg);
        parenthesizeIfNeeded(node, node.getLeft());
        builder.append(" ");
        builder.append(node.getOperator());
        builder.append(" ");
        parenthesizeIfNeeded(node, node.getRight());
        return null;
    }

    private void parenthesizeIfNeeded(AbstractBinaryCodeNode parent, ICodeNode operand) throws AlgebricksException {
        if (operand.getType() == CodeNodeType.BINARY_ARITHMETIC) {
            AbstractBinaryCodeNode binaryOperand = (AbstractBinaryCodeNode) operand;
            if (isTerm(parent) && isFactor(binaryOperand)) {
                builder.append("(");
                operand.accept(this, 0);
                builder.append(")");
                return;
            }
        }
        operand.accept(this, 0);
    }

    private boolean isTerm(AbstractBinaryCodeNode node) {
        switch (node.getOperator()) {
            case MultiplyScalarOperatorCodeNode.OPERATOR:
            case DivideScalarOperatorCodeNode.OPERATOR:
            case ModScalarOperatorCodeNode.OPERATOR:
                return true;
            default:
                return false;
        }
    }

    private boolean isFactor(AbstractBinaryCodeNode node) {
        switch (node.getOperator()) {
            case AddScalarOperatorCodeNode.OPERATOR:
            case SubtractScalarOperatorCodeNode.OPERATOR:
                return true;
            default:
                return false;
        }
    }

    @Override
    public Void visit(AbstractBuiltinFunction node, Integer arg) throws AlgebricksException {
        indent(arg);
        ICodeNode[] args = node.getArgs();
        builder.append(node.getFunctionName());
        builder.append('(');
        if (args.length > 0) {
            args[0].accept(this, 0);
            for (int i = 1; i < args.length; i++) {
                builder.append(", ");
                args[i].accept(this, 0);
            }
        }
        builder.append(')');
        return null;
    }

    @Override
    public Void visit(IdentifierCodeNode node, Integer arg) throws AlgebricksException {
        builder.append(node.toString());
        return null;
    }

    @Override
    public Void visit(IfCodeNode node, Integer arg) throws AlgebricksException {
        append(arg, "if (");
        node.getCondition().accept(this, 0);
        builder.append(") {\n");
        node.getThenBlock().accept(this, arg);

        BlockCodeNode elseBlock = node.getElseBlock();
        if (!elseBlock.isEmpty()) {
            append(arg, "} else {\n");
            elseBlock.accept(this, arg);
        }
        append(arg, "}");
        return null;
    }

    @Override
    public Void visit(WhileCodeNode node, Integer arg) throws AlgebricksException {
        append(arg, "while (");
        node.getCondition().accept(this, 0);
        builder.append(") {\n");
        node.getLoopBlock().accept(this, arg);
        append(arg, "}");
        return null;
    }

    @Override
    public Void visit(ReturnCodeNode node, Integer arg) throws AlgebricksException {
        append(arg, "return ");
        node.getReturnExpression().accept(this, 0);
        return null;
    }

    @Override
    public Void visit(BreakCodeNode node, Integer arg) throws AlgebricksException {
        append(arg, "break");
        return null;
    }

    @Override
    public Void visit(AbstractMemberCodeNode methodCallCodeNode, Integer arg) throws AlgebricksException {
        append(arg, methodCallCodeNode.toString());
        return null;
    }

    @Override
    public Void visit(MainFunctionCodeNode mainFunctionCodeNode, Integer arg) throws AlgebricksException {
        addReadersSchemasAsComment(builder, mainFunctionCodeNode.getPaths());
        builder.append("function ");
        builder.append(mainFunctionCodeNode.getFunctionName());
        builder.append(" (");

        List<IdentifierCodeNode> mainArgs = mainFunctionCodeNode.getArgs();
        builder.append(mainArgs.get(0));
        for (int i = 1; i < mainArgs.size(); i++) {
            builder.append(", ");
            builder.append(mainArgs.get(i));
        }
        builder.append(") {\n");
        mainFunctionCodeNode.getBlock().accept(this, arg);
        builder.append("}");
        return null;
    }

    private void addReadersSchemasAsComment(StringBuilder builder, ARecordType[] paths) {
        for (int i = 0; i < paths.length; i++) {
            builder.append("//");
            builder.append(CodeGenTemplates.READER_OBJECT);
            builder.append(i);
            builder.append(": ");
            paths[i].accept(typePrinter, builder);
            builder.append('\n');
        }
    }

    @Override
    public Void visit(BlockCodeNode blockCodeNode, Integer arg) throws AlgebricksException {
        visitBlock(blockCodeNode.getHeadBlock(), arg + 1);
        visitBlock(blockCodeNode.getTailBlock(), arg + 1);
        return null;
    }

    private void visitBlock(List<ICodeNode> block, int arg) throws AlgebricksException {
        for (ICodeNode node : block) {
            node.accept(this, arg);
            char lastChar = builder.charAt(builder.length() - 1);
            if (lastChar != '}') {
                builder.append(';');
            }
            builder.append('\n');
        }
    }

    private void indent(int numOfIndent) {
        for (int i = 0; i < numOfIndent; i++) {
            builder.append(INDENT);
        }
    }

    private void append(int numOfIndent, String s) {
        indent(numOfIndent);
        builder.append(s);
    }
}
