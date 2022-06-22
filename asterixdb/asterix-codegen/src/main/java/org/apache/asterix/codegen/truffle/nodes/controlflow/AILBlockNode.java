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
package org.apache.asterix.codegen.truffle.nodes.controlflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.codegen.truffle.nodes.AILStatementNode;
import org.apache.asterix.codegen.truffle.nodes.local.AILScopedNode;
import org.apache.asterix.codegen.truffle.nodes.local.AILWriteLocalVariableNode;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.BlockNode;
import com.oracle.truffle.api.nodes.BlockNode.ElementExecutor;
import com.oracle.truffle.api.nodes.ControlFlowException;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.NodeUtil;
import com.oracle.truffle.api.nodes.NodeVisitor;

/**
 * A statement node that just executes a list of other statements.
 */
@NodeInfo(shortName = "block", description = "The node implementing a source code block")
public final class AILBlockNode extends AILStatementNode implements BlockNode.ElementExecutor<AILStatementNode> {

    /**
     * The block of child nodes. Using the block node allows Truffle to split the block into
     * multiple groups for compilation if the method is too big. This is an optional API.
     * Alternatively, you may just use your own block node, with a
     * {@link com.oracle.truffle.api.nodes.Node.Children @Children} field. However, this prevents
     * Truffle from compiling big methods, so these methods might fail to compile with a compilation
     * bailout.
     */
    @Child
    private BlockNode<AILStatementNode> block;

    /**
     * All declared variables visible from this block (including all parent blocks). Variables
     * declared in this block only are from zero index up to {@link #parentBlockIndex} (exclusive).
     */
    @CompilationFinal(dimensions = 1)
    private AILWriteLocalVariableNode[] writeNodesCache;

    /**
     * Index of the parent block's variables in the {@link #writeNodesCache list of variables}.
     */
    @CompilationFinal
    private int parentBlockIndex = -1;

    public AILBlockNode(AILStatementNode[] bodyNodes) {
        /*
         * Truffle block nodes cannot be empty, that is why we just set the entire block to null if
         * there are no elements. This is good practice as it safes memory.
         */
        this.block = bodyNodes.length > 0 ? BlockNode.create(bodyNodes, this) : null;
    }

    /**
     * Execute all block statements. The block node makes sure that {@link ExplodeLoop full
     * unrolling} of the loop is triggered during compilation. This allows the
     * {@link AILStatementNode#executeVoid} method of all children to be inlined.
     */
    @Override
    public void executeVoid(VirtualFrame frame) {
        if (this.block != null) {
            this.block.executeVoid(frame, BlockNode.NO_ARGUMENT);
        }
    }

    public List<AILStatementNode> getStatements() {
        if (block == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(Arrays.asList(block.getElements()));
    }

    /**
     * Truffle nodes don't have a fixed execute signature. The {@link ElementExecutor} interface
     * tells the framework how block element nodes should be executed. The executor allows to add a
     * custom exception handler for each element, e.g. to handle a specific
     * {@link ControlFlowException} or to pass a customizable argument, that allows implement
     * startsWith semantics if needed. For SL we don't need to pass any argument as we just have
     * plain block nodes, therefore we pass {@link BlockNode#NO_ARGUMENT}. In our case the executor
     * does not need to remember any state so we reuse a singleton instance.
     */
    @Override
    public void executeVoid(VirtualFrame frame, AILStatementNode node, int index, int argument) {
        node.executeVoid(frame);
    }

    /**
     * All declared local variables accessible in this block. Variables declared in parent blocks
     * are included.
     */
    public AILWriteLocalVariableNode[] getDeclaredLocalVariables() {
        AILWriteLocalVariableNode[] writeNodes = writeNodesCache;
        if (writeNodes == null) {
            CompilerDirectives.transferToInterpreterAndInvalidate();
            writeNodesCache = writeNodes = findDeclaredLocalVariables();
        }
        return writeNodes;
    }

    public int getParentBlockIndex() {
        return parentBlockIndex;
    }

    private AILWriteLocalVariableNode[] findDeclaredLocalVariables() {
        if (block == null) {
            return new AILWriteLocalVariableNode[] {};
        }
        // Search for those write nodes, which declare variables
        List<AILWriteLocalVariableNode> writeNodes = new ArrayList<>(4);
        int[] varsIndex = new int[] { 0 };
        NodeUtil.forEachChild(block, new NodeVisitor() {
            @Override
            public boolean visit(Node node) {
                if (node instanceof WrapperNode) {
                    NodeUtil.forEachChild(node, this);
                    return true;
                }
                if (node instanceof AILScopedNode) {
                    AILScopedNode scopedNode = (AILScopedNode) node;
                    scopedNode.setVisibleVariablesIndexOnEnter(varsIndex[0]);
                }
                // Do not enter any nested blocks.
                if (!(node instanceof AILBlockNode)) {
                    NodeUtil.forEachChild(node, this);
                }
                // Write to a variable is a declaration unless it exists already in a parent scope.
                if (node instanceof AILWriteLocalVariableNode) {
                    AILWriteLocalVariableNode wn = (AILWriteLocalVariableNode) node;
                    if (wn.isDeclaration()) {
                        writeNodes.add(wn);
                        varsIndex[0]++;
                    }
                }
                if (node instanceof AILScopedNode) {
                    AILScopedNode scopedNode = (AILScopedNode) node;
                    scopedNode.setVisibleVariablesIndexOnExit(varsIndex[0]);
                }
                return true;
            }
        });
        Node parentBlock = findBlock();
        AILWriteLocalVariableNode[] parentVariables = null;
        if (parentBlock instanceof AILBlockNode) {
            parentVariables = ((AILBlockNode) parentBlock).getDeclaredLocalVariables();
        }
        AILWriteLocalVariableNode[] variables = writeNodes.toArray(new AILWriteLocalVariableNode[writeNodes.size()]);
        parentBlockIndex = variables.length;
        if (parentVariables == null || parentVariables.length == 0) {
            return variables;
        } else {
            int parentVariablesIndex = ((AILBlockNode) parentBlock).getParentBlockIndex();
            int visibleVarsIndex = getVisibleVariablesIndexOnEnter();
            int allVarsLength = variables.length + visibleVarsIndex + parentVariables.length - parentVariablesIndex;
            AILWriteLocalVariableNode[] allVariables = Arrays.copyOf(variables, allVarsLength);
            System.arraycopy(parentVariables, 0, allVariables, variables.length, visibleVarsIndex);
            System.arraycopy(parentVariables, parentVariablesIndex, allVariables, variables.length + visibleVarsIndex,
                    parentVariables.length - parentVariablesIndex);
            return allVariables;
        }
    }

}
