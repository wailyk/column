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
package org.apache.asterix.codegen.truffle.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Token;
import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.nodes.AILExpressionNode;
import org.apache.asterix.codegen.truffle.nodes.AILRootNode;
import org.apache.asterix.codegen.truffle.nodes.AILStatementNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILBlockNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILBreakNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILContinueNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILDebuggerNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILFunctionBodyNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILIfNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILReturnNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILWhileNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILAddNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILAggregateAddNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILBooleanLiteral;
import org.apache.asterix.codegen.truffle.nodes.expression.AILDivNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILDoubleLiteralNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILEqualNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILFunctionLiteralNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILInvokeNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILLessOrEqualNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILLessThanNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILLogicalAndNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILLogicalNotNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILLogicalOrNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILLongLiteralNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILMaxNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILMinNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILMissingLiteral;
import org.apache.asterix.codegen.truffle.nodes.expression.AILModNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILMulNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILNegationNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILNullLiteral;
import org.apache.asterix.codegen.truffle.nodes.expression.AILParenExpressionNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILReadPropertyNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILReadPropertyNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILStringLiteralNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILStringRuntimeLiteral;
import org.apache.asterix.codegen.truffle.nodes.expression.AILSubNodeGen;
import org.apache.asterix.codegen.truffle.nodes.expression.AILWritePropertyNode;
import org.apache.asterix.codegen.truffle.nodes.expression.AILWritePropertyNodeGen;
import org.apache.asterix.codegen.truffle.nodes.local.AILReadArgumentNode;
import org.apache.asterix.codegen.truffle.nodes.local.AILReadLocalVariableNode;
import org.apache.asterix.codegen.truffle.nodes.local.AILReadLocalVariableNodeGen;
import org.apache.asterix.codegen.truffle.nodes.local.AILWriteLocalVariableNode;
import org.apache.asterix.codegen.truffle.nodes.local.AILWriteLocalVariableNodeGen;
import org.apache.asterix.codegen.truffle.nodes.util.AILUnboxNodeGen;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.source.SourceSection;

/**
 * Helper class used by the SL {@link Parser} to create nodes. The code is factored out of the
 * automatically generated parser to keep the attributed grammar of SL small.
 */
public class AILNodeFactory {

    /**
     * Local variable names that are visible in the current block. Variables are not visible outside
     * of their defining block, to prevent the usage of undefined variables. Because of that, we can
     * decide during parsing if a name references a local variable or is a function name.
     */
    static class LexicalScope {
        protected final LexicalScope outer;
        protected final Map<String, FrameSlot> locals;

        LexicalScope(LexicalScope outer) {
            this.outer = outer;
            this.locals = new HashMap<>();
            if (outer != null) {
                locals.putAll(outer.locals);
            }
        }
    }

    /* State while parsing a source unit. */
    private final Source source;
    private final Map<String, RootCallTarget> allFunctions;

    /* State while parsing a function. */
    private int functionStartPos;
    private String functionName;
    private int functionBodyStartPos; // includes parameter list
    private int parameterCount;
    private FrameDescriptor frameDescriptor;
    private List<AILStatementNode> methodNodes;

    /* State while parsing a block. */
    private LexicalScope lexicalScope;
    private final AILLanguage language;

    /* AsterixString */
    private final AMutableString mutableString;
    private final AStringSerializerDeserializer stringSerDer;

    public AILNodeFactory(AILLanguage language, Source source) {
        this.language = language;
        this.source = source;
        this.allFunctions = new HashMap<>();
        stringSerDer = new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
        mutableString = new AMutableString("");
    }

    public Map<String, RootCallTarget> getAllFunctions() {
        return allFunctions;
    }

    public void startFunction(Token nameToken, Token bodyStartToken) {
        assert functionStartPos == 0;
        assert functionName == null;
        assert functionBodyStartPos == 0;
        assert parameterCount == 0;
        assert frameDescriptor == null;
        assert lexicalScope == null;

        functionStartPos = nameToken.getStartIndex();
        functionName = nameToken.getText();
        functionBodyStartPos = bodyStartToken.getStartIndex();
        frameDescriptor = new FrameDescriptor();
        methodNodes = new ArrayList<>();
        startBlock();
    }

    public void addFormalParameter(Token nameToken) {
        /*
         * Method parameters are assigned to local variables at the beginning of the method. This
         * ensures that accesses to parameters are specialized the same way as local variables are
         * specialized.
         */
        final AILReadArgumentNode readArg = new AILReadArgumentNode(parameterCount);
        readArg.setSourceSection(nameToken.getStartIndex(), nameToken.getText().length());
        AILExpressionNode assignment = createAssignment(createStringLiteral(nameToken, false), readArg, parameterCount);
        methodNodes.add(assignment);
        parameterCount++;
    }

    public void finishFunction(AILStatementNode bodyNode) {
        if (bodyNode == null) {
            // a state update that would otherwise be performed by finishBlock
            lexicalScope = lexicalScope.outer;
        } else {
            methodNodes.add(bodyNode);
            final int bodyEndPos = bodyNode.getSourceEndIndex();
            final SourceSection functionSrc = source.createSection(functionStartPos, bodyEndPos - functionStartPos);
            final AILStatementNode methodBlock =
                    finishBlock(methodNodes, parameterCount, functionBodyStartPos, bodyEndPos - functionBodyStartPos);
            assert lexicalScope == null : "Wrong scoping of blocks in parser";

            final AILFunctionBodyNode functionBodyNode = new AILFunctionBodyNode(methodBlock);
            functionBodyNode.setSourceSection(functionSrc.getCharIndex(), functionSrc.getCharLength());

            final AILRootNode rootNode =
                    new AILRootNode(language, frameDescriptor, functionBodyNode, functionSrc, functionName);
            allFunctions.put(functionName, Truffle.getRuntime().createCallTarget(rootNode));
        }

        functionStartPos = 0;
        functionName = null;
        functionBodyStartPos = 0;
        parameterCount = 0;
        frameDescriptor = null;
        lexicalScope = null;
    }

    public void startBlock() {
        lexicalScope = new LexicalScope(lexicalScope);
    }

    public AILStatementNode finishBlock(List<AILStatementNode> bodyNodes, int startPos, int length) {
        return finishBlock(bodyNodes, 0, startPos, length);
    }

    public AILStatementNode finishBlock(List<AILStatementNode> bodyNodes, int skipCount, int startPos, int length) {
        lexicalScope = lexicalScope.outer;

        if (containsNull(bodyNodes)) {
            return null;
        }

        List<AILStatementNode> flattenedNodes = new ArrayList<>(bodyNodes.size());
        flattenBlocks(bodyNodes, flattenedNodes);
        int n = flattenedNodes.size();
        for (int i = skipCount; i < n; i++) {
            AILStatementNode statement = flattenedNodes.get(i);
            if (statement.hasSource() && !isHaltInCondition(statement)) {
                statement.addStatementTag();
            }
        }
        AILBlockNode blockNode = new AILBlockNode(flattenedNodes.toArray(new AILStatementNode[flattenedNodes.size()]));
        blockNode.setSourceSection(startPos, length);
        return blockNode;
    }

    private static boolean isHaltInCondition(AILStatementNode statement) {
        return (statement instanceof AILIfNode) || (statement instanceof AILWhileNode);
    }

    private void flattenBlocks(Iterable<? extends AILStatementNode> bodyNodes, List<AILStatementNode> flattenedNodes) {
        for (AILStatementNode n : bodyNodes) {
            if (n instanceof AILBlockNode) {
                flattenBlocks(((AILBlockNode) n).getStatements(), flattenedNodes);
            } else {
                flattenedNodes.add(n);
            }
        }
    }

    /**
     * Returns an {@link AILDebuggerNode} for the given token.
     *
     * @param debuggerToken The token containing the debugger node's info.
     * @return A SLDebuggerNode for the given token.
     */
    AILStatementNode createDebugger(Token debuggerToken) {
        final AILDebuggerNode debuggerNode = new AILDebuggerNode();
        srcFromToken(debuggerNode, debuggerToken);
        return debuggerNode;
    }

    /**
     * Returns an {@link AILBreakNode} for the given token.
     *
     * @param breakToken The token containing the break node's info.
     * @return A SLBreakNode for the given token.
     */
    public AILStatementNode createBreak(Token breakToken) {
        final AILBreakNode breakNode = new AILBreakNode();
        srcFromToken(breakNode, breakToken);
        return breakNode;
    }

    /**
     * Returns an {@link AILContinueNode} for the given token.
     *
     * @param continueToken The token containing the continue node's info.
     * @return A SLContinueNode built using the given token.
     */
    public AILStatementNode createContinue(Token continueToken) {
        final AILContinueNode continueNode = new AILContinueNode();
        srcFromToken(continueNode, continueToken);
        return continueNode;
    }

    /**
     * Returns an {@link AILWhileNode} for the given parameters.
     *
     * @param whileToken    The token containing the while node's info
     * @param conditionNode The conditional node for this while loop
     * @param bodyNode      The body of the while loop
     * @return A SLWhileNode built using the given parameters. null if either conditionNode or
     * bodyNode is null.
     */
    public AILStatementNode createWhile(Token whileToken, AILExpressionNode conditionNode, AILStatementNode bodyNode) {
        if (conditionNode == null || bodyNode == null) {
            return null;
        }

        conditionNode.addStatementTag();
        final int start = whileToken.getStartIndex();
        final int end = bodyNode.getSourceEndIndex();
        final AILWhileNode whileNode = new AILWhileNode(conditionNode, bodyNode);
        whileNode.setSourceSection(start, end - start);
        return whileNode;
    }

    /**
     * Returns an {@link AILIfNode} for the given parameters.
     *
     * @param ifToken       The token containing the if node's info
     * @param conditionNode The condition node of this if statement
     * @param thenPartNode  The then part of the if
     * @param elsePartNode  The else part of the if (null if no else part)
     * @return An SLIfNode for the given parameters. null if either conditionNode or thenPartNode is
     * null.
     */
    public AILStatementNode createIf(Token ifToken, AILExpressionNode conditionNode, AILStatementNode thenPartNode,
            AILStatementNode elsePartNode) {
        if (conditionNode == null || thenPartNode == null) {
            return null;
        }

        conditionNode.addStatementTag();
        final int start = ifToken.getStartIndex();
        final int end = elsePartNode == null ? thenPartNode.getSourceEndIndex() : elsePartNode.getSourceEndIndex();
        final AILIfNode ifNode = new AILIfNode(conditionNode, thenPartNode, elsePartNode);
        ifNode.setSourceSection(start, end - start);
        return ifNode;
    }

    /**
     * Returns an {@link AILReturnNode} for the given parameters.
     *
     * @param t         The token containing the return node's info
     * @param valueNode The value of the return (null if not returning a value)
     * @return An SLReturnNode for the given parameters.
     */
    public AILStatementNode createReturn(Token t, AILExpressionNode valueNode) {
        final int start = t.getStartIndex();
        final int length = valueNode == null ? t.getText().length() : valueNode.getSourceEndIndex() - start;
        final AILReturnNode returnNode = new AILReturnNode(valueNode);
        returnNode.setSourceSection(start, length);
        return returnNode;
    }

    /**
     * Returns the corresponding subclass of {@link AILExpressionNode} for binary expressions. </br>
     * These nodes are currently not instrumented.
     *
     * @param opToken   The operator of the binary expression
     * @param leftNode  The left node of the expression
     * @param rightNode The right node of the expression
     * @return A subclass of AILExpressionNode using the given parameters based on the given opToken.
     * null if either leftNode or rightNode is null.
     */
    public AILExpressionNode createBinary(Token opToken, AILExpressionNode leftNode, AILExpressionNode rightNode) {
        if (leftNode == null || rightNode == null) {
            return null;
        }
        final AILExpressionNode leftUnboxed = AILUnboxNodeGen.create(leftNode);
        final AILExpressionNode rightUnboxed = AILUnboxNodeGen.create(rightNode);

        final AILExpressionNode result;
        switch (opToken.getText()) {
            case "+":
                result = AILAddNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case "++":
                result = AILAggregateAddNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case "*":
                result = AILMulNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case "/":
                result = AILDivNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case "-":
                result = AILSubNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case "<":
                result = AILLessThanNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case "<=":
                result = AILLessOrEqualNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case ">":
                result = AILLessThanNodeGen.create(rightUnboxed, leftUnboxed);
                break;
            case ">=":
                result = AILLessOrEqualNodeGen.create(rightUnboxed, leftUnboxed);
                break;
            case "==":
                result = AILEqualNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case "!=":
                result = AILLogicalNotNodeGen.create(AILEqualNodeGen.create(leftUnboxed, rightUnboxed));
                break;
            case "&&":
                result = new AILLogicalAndNode(leftUnboxed, rightUnboxed);
                break;
            case "||":
                result = new AILLogicalOrNode(leftUnboxed, rightUnboxed);
                break;
            case "/\\":
                result = AILMaxNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case "\\/":
                result = AILMinNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            case "%":
                result = AILModNodeGen.create(leftUnboxed, rightUnboxed);
                break;
            default:
                throw new RuntimeException("unexpected operation: " + opToken.getText());
        }

        int start = leftNode.getSourceCharIndex();
        int length = rightNode.getSourceEndIndex() - start;
        result.setSourceSection(start, length);
        result.addExpressionTag();

        return result;
    }

    /**
     * Returns an {@link AILInvokeNode} for the given parameters.
     *
     * @param functionNode   The function being called
     * @param parameterNodes The parameters of the function call
     * @param finalToken     A token used to determine the end of the sourceSelection for this call
     * @return An AILInvokeNode for the given parameters. null if functionNode or any of the
     * parameterNodes are null.
     */
    public AILExpressionNode createCall(AILExpressionNode functionNode, List<AILExpressionNode> parameterNodes,
            Token finalToken) {
        if (functionNode == null || containsNull(parameterNodes)) {
            return null;
        }

        final AILExpressionNode result =
                new AILInvokeNode(functionNode, parameterNodes.toArray(new AILExpressionNode[parameterNodes.size()]));

        final int startPos = functionNode.getSourceCharIndex();
        final int endPos = finalToken.getStartIndex() + finalToken.getText().length();
        result.setSourceSection(startPos, endPos - startPos);
        result.addExpressionTag();

        return result;
    }

    /**
     * Returns an {@link AILWriteLocalVariableNode} for the given parameters.
     *
     * @param nameNode  The name of the variable being assigned
     * @param valueNode The value to be assigned
     * @return An AILExpressionNode for the given parameters. null if nameNode or valueNode is null.
     */
    public AILExpressionNode createAssignment(AILExpressionNode nameNode, AILExpressionNode valueNode) {
        return createAssignment(nameNode, valueNode, null);
    }

    /**
     * Returns an {@link AILWriteLocalVariableNode} for the given parameters.
     *
     * @param nameNode      The name of the variable being assigned
     * @param valueNode     The value to be assigned
     * @param argumentIndex null or index of the argument the assignment is assigning
     * @return An AILExpressionNode for the given parameters. null if nameNode or valueNode is null.
     */
    public AILExpressionNode createAssignment(AILExpressionNode nameNode, AILExpressionNode valueNode,
            Integer argumentIndex) {
        if (nameNode == null || valueNode == null) {
            return null;
        }

        String name = ((AILStringLiteralNode) nameNode).executeGeneric(null);
        FrameSlot frameSlot = frameDescriptor.findOrAddFrameSlot(name, argumentIndex, FrameSlotKind.Illegal);
        FrameSlot existingSlot = lexicalScope.locals.put(name, frameSlot);
        boolean newVariable = existingSlot == null;
        final AILExpressionNode result =
                AILWriteLocalVariableNodeGen.create(valueNode, frameSlot, nameNode, newVariable);

        if (valueNode.hasSource()) {
            final int start = nameNode.getSourceCharIndex();
            final int length = valueNode.getSourceEndIndex() - start;
            result.setSourceSection(start, length);
        }
        if (argumentIndex == null) {
            result.addExpressionTag();
        }

        return result;
    }

    /**
     * Returns a {@link AILReadLocalVariableNode} if this read is a local variable or a
     * {@link AILFunctionLiteralNode} if this read is global. In SL, the only global names are
     * functions.
     *
     * @param nameNode The name of the variable/function being read
     * @return either:
     * <ul>
     * <li>A AILReadLocalVariableNode representing the local variable being read.</li>
     * <li>A AILFunctionLiteralNode representing the function definition.</li>
     * <li>null if nameNode is null.</li>
     * </ul>
     */
    public AILExpressionNode createRead(AILExpressionNode nameNode) {
        if (nameNode == null) {
            return null;
        }

        String name = ((AILStringLiteralNode) nameNode).executeGeneric(null);
        final AILExpressionNode result;
        final FrameSlot frameSlot = lexicalScope.locals.get(name);
        if (frameSlot != null) {
            /* Read of a local variable. */
            result = AILReadLocalVariableNodeGen.create(frameSlot);
        } else {
            /* Read of a global name. In our language, the only global names are functions. */
            result = new AILFunctionLiteralNode(name);
        }
        result.setSourceSection(nameNode.getSourceCharIndex(), nameNode.getSourceLength());
        result.addExpressionTag();
        return result;
    }

    public AILExpressionNode createMissingLiteral(Token literalToken) {
        AILExpressionNode result = new AILMissingLiteral();
        srcFromToken(result, literalToken);
        return result;
    }

    public AILExpressionNode createNullLiteral(Token literalToken) {
        AILExpressionNode result = new AILNullLiteral();
        srcFromToken(result, literalToken);
        return result;
    }

    public AILExpressionNode createBooleanLiteral(Token literalToken) {
        AILExpressionNode result = new AILBooleanLiteral(Boolean.parseBoolean(literalToken.getText()));
        srcFromToken(result, literalToken);
        return result;
    }

    public AILExpressionNode createStringLiteral(Token literalToken, boolean removeQuotes) {
        /* Remove the trailing and ending " */
        String literal = literalToken.getText();
        if (removeQuotes) {
            assert literal.length() >= 2 && literal.startsWith("\"") && literal.endsWith("\"");
            literal = literal.substring(1, literal.length() - 1);
        }

        final AILStringLiteralNode result = new AILStringLiteralNode(literal.intern());
        srcFromToken(result, literalToken);
        result.addExpressionTag();
        return result;
    }

    public AILExpressionNode createStringRuntimeLiteral(Token literalToken, boolean removeQuotes) {
        /* Remove the trailing and ending " */
        String literal = literalToken.getText();
        if (removeQuotes) {
            assert literal.length() >= 4 && literal.startsWith("`") && literal.endsWith("`");
            literal = literal.substring(1, literal.length() - 1);
        }
        final ArrayBackedValueStorage stringStorage = new ArrayBackedValueStorage();
        mutableString.setValue(literal);
        try {
            stringSerDer.serialize(mutableString, stringStorage.getDataOutput());
        } catch (IOException e) {
            throw new RuntimeException("Error serializing a string", e);
        }

        final AILStringRuntimeLiteral result = new AILStringRuntimeLiteral(stringStorage);
        srcFromToken(result, literalToken);
        result.addExpressionTag();
        return result;
    }

    public AILExpressionNode createNumericLiteral(Token literalToken) {
        AILExpressionNode result = new AILLongLiteralNode(Long.parseLong(literalToken.getText()));
        srcFromToken(result, literalToken);
        result.addExpressionTag();
        return result;
    }

    public AILExpressionNode createDoubleLiteral(Token literalToken) {
        AILExpressionNode result = new AILDoubleLiteralNode(Double.parseDouble(literalToken.getText()));
        srcFromToken(result, literalToken);
        result.addExpressionTag();
        return result;
    }

    public AILExpressionNode createParenExpression(AILExpressionNode expressionNode, int start, int length) {
        if (expressionNode == null) {
            return null;
        }

        final AILParenExpressionNode result = new AILParenExpressionNode(expressionNode);
        result.setSourceSection(start, length);
        return result;
    }

    public AILExpressionNode createNotExpression(AILExpressionNode valueNode, int start) {
        AILExpressionNode result = AILLogicalNotNodeGen.create(valueNode);
        result.setSourceSection(start, valueNode.getSourceLength() + 1);
        result.addExpressionTag();
        return result;
    }

    public AILExpressionNode createNegatedExpression(AILExpressionNode valueNode, int start) {
        AILExpressionNode result;
        //        if (valueNode instanceof AILDoubleLiteralNode) {
        //            result = new AILDoubleLiteralNode(-((AILDoubleLiteralNode) valueNode).executeDouble(null));
        //        } else if (valueNode instanceof AILLongLiteralNode) {
        //            result = new AILLongLiteralNode(-((AILLongLiteralNode) valueNode).executeLong(null));
        //        } else {
        //            result = AILNegationNodeGen.create(valueNode);
        //        }
        result = AILNegationNodeGen.create(valueNode);
        result.setSourceSection(start, valueNode.getSourceLength() + 1);
        result.addExpressionTag();
        return result;
    }

    /**
     * Returns an {@link AILReadPropertyNode} for the given parameters.
     *
     * @param receiverNode The receiver of the property access
     * @param nameNode     The name of the property being accessed
     * @return An SLExpressionNode for the given parameters. null if receiverNode or nameNode is
     * null.
     */
    public AILExpressionNode createReadProperty(AILExpressionNode receiverNode, AILExpressionNode nameNode) {
        if (receiverNode == null || nameNode == null) {
            return null;
        }

        final AILExpressionNode result = AILReadPropertyNodeGen.create(receiverNode, nameNode);

        final int startPos = receiverNode.getSourceCharIndex();
        final int endPos = nameNode.getSourceEndIndex();
        result.setSourceSection(startPos, endPos - startPos);
        result.addExpressionTag();

        return result;
    }

    /**
     * Returns an {@link AILWritePropertyNode} for the given parameters.
     *
     * @param receiverNode The receiver object of the property assignment
     * @param nameNode     The name of the property being assigned
     * @param valueNode    The value to be assigned
     * @return An SLExpressionNode for the given parameters. null if receiverNode, nameNode or
     * valueNode is null.
     */
    public AILExpressionNode createWriteProperty(AILExpressionNode receiverNode, AILExpressionNode nameNode,
            AILExpressionNode valueNode) {
        if (receiverNode == null || nameNode == null || valueNode == null) {
            return null;
        }

        final AILExpressionNode result = AILWritePropertyNodeGen.create(receiverNode, nameNode, valueNode);

        final int start = receiverNode.getSourceCharIndex();
        final int length = valueNode.getSourceEndIndex() - start;
        result.setSourceSection(start, length);
        result.addExpressionTag();

        return result;
    }

    /**
     * Creates source description of a single token.
     */
    private static void srcFromToken(AILStatementNode node, Token token) {
        node.setSourceSection(token.getStartIndex(), token.getText().length());
    }

    /**
     * Checks whether a list contains a null.
     */
    private static boolean containsNull(List<?> list) {
        for (Object e : list) {
            if (e == null) {
                return true;
            }
        }
        return false;
    }

}
