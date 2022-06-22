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
package org.apache.asterix.codegen.truffle.nodes;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.builtins.AILBuiltinNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILBlockNode;
import org.apache.asterix.codegen.truffle.nodes.controlflow.AILFunctionBodyNode;
import org.apache.asterix.codegen.truffle.nodes.local.AILReadArgumentNode;
import org.apache.asterix.codegen.truffle.nodes.local.AILWriteLocalVariableNode;
import org.apache.asterix.codegen.truffle.runtime.AILContext;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.instrumentation.InstrumentableNode;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.NodeUtil;
import com.oracle.truffle.api.nodes.NodeVisitor;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;

/**
 * The root of all SL execution trees. It is a Truffle requirement that the tree root extends the
 * class {@link RootNode}. This class is used for both builtin and user-defined functions. For
 * builtin functions, the {@link #bodyNode} is a subclass of {@link AILBuiltinNode}. For user-defined
 * functions, the {@link #bodyNode} is a {@link AILFunctionBodyNode}.
 */
@NodeInfo(language = "SL", description = "The root of all SL execution trees")
public class AILRootNode extends RootNode {
    /** The function body that is executed, and specialized during execution. */
    @Child
    private AILExpressionNode bodyNode;

    /** The name of the function, for printing purposes only. */
    private final String name;

    private boolean isCloningAllowed;

    private final SourceSection sourceSection;

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private volatile AILWriteLocalVariableNode[] argumentNodesCache;

    public AILRootNode(AILLanguage language, FrameDescriptor frameDescriptor, AILExpressionNode bodyNode,
            SourceSection sourceSection, String name) {
        super(language, frameDescriptor);
        this.bodyNode = bodyNode;
        this.name = name;
        this.sourceSection = sourceSection;
    }

    @Override
    public SourceSection getSourceSection() {
        return sourceSection;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        assert AILContext.get(this) != null;
        return bodyNode.executeGeneric(frame);
    }

    public AILExpressionNode getBodyNode() {
        return bodyNode;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setCloningAllowed(boolean isCloningAllowed) {
        this.isCloningAllowed = isCloningAllowed;
    }

    @Override
    public boolean isCloningAllowed() {
        return isCloningAllowed;
    }

    @Override
    public String toString() {
        return "root " + name;
    }

    public final AILWriteLocalVariableNode[] getDeclaredArguments() {
        AILWriteLocalVariableNode[] argumentNodes = argumentNodesCache;
        if (argumentNodes == null) {
            CompilerDirectives.transferToInterpreterAndInvalidate();
            argumentNodesCache = argumentNodes = findArgumentNodes();
        }
        return argumentNodes;
    }

    private AILWriteLocalVariableNode[] findArgumentNodes() {
        List<AILWriteLocalVariableNode> writeArgNodes = new ArrayList<>(4);
        NodeUtil.forEachChild(this.getBodyNode(), new NodeVisitor() {

            private AILWriteLocalVariableNode wn; // The current write node containing a slot

            @Override
            public boolean visit(Node node) {
                // When there is a write node, search for SLReadArgumentNode among its children:
                if (node instanceof InstrumentableNode.WrapperNode) {
                    return NodeUtil.forEachChild(node, this);
                }
                if (node instanceof AILWriteLocalVariableNode) {
                    wn = (AILWriteLocalVariableNode) node;
                    boolean all = NodeUtil.forEachChild(node, this);
                    wn = null;
                    return all;
                } else if (wn != null && (node instanceof AILReadArgumentNode)) {
                    writeArgNodes.add(wn);
                    return true;
                } else if (wn == null && (node instanceof AILStatementNode
                        && !(node instanceof AILBlockNode || node instanceof AILFunctionBodyNode))) {
                    // A different SL node - we're done.
                    return false;
                } else {
                    return NodeUtil.forEachChild(node, this);
                }
            }
        });
        return writeArgNodes.toArray(new AILWriteLocalVariableNode[writeArgNodes.size()]);
    }

}
