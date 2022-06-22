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
package org.apache.asterix.optimizer.rules.codegen.node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.optimizer.rules.codegen.node.expression.binary.AssignOperatorCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;

public class BlockCodeNode extends AbstractCodeNode {
    private static final long serialVersionUID = 3125862968492675847L;
    public static final BlockCodeNode EMPTY = new BlockCodeNode();
    private final List<ICodeNode> headBlock;
    private final List<ICodeNode> tailBlock;
    private final Map<IdentifierCodeNode, ICodeNode> blockVariables;
    private final Map<IdentifierCodeNode, IdentifierCodeNode> boundReaders;
    private final BlockCodeNode parent;
    private final LogicalOperatorTag tag;

    private BlockCodeNode() {
        super(GENERATED_LOCATION);
        this.parent = null;
        this.tag = null;
        this.headBlock = Collections.emptyList();
        this.tailBlock = Collections.emptyList();
        this.blockVariables = Collections.emptyMap();
        this.boundReaders = Collections.emptyMap();
    }

    public BlockCodeNode(BlockCodeNode parent, LogicalOperatorTag tag) {
        super(GENERATED_LOCATION);
        this.parent = parent;
        this.tag = tag;
        this.headBlock = new ArrayList<>();
        this.tailBlock = new ArrayList<>();
        this.blockVariables = new HashMap<>();
        this.boundReaders = new HashMap<>();
    }

    public List<ICodeNode> getHeadBlock() {
        return headBlock;
    }

    public List<ICodeNode> getTailBlock() {
        return tailBlock;
    }

    public void appendNode(ICodeNode node) {
        headBlock.add(node);
    }

    public void appendNodes(List<ICodeNode> nodes) {
        headBlock.addAll(nodes);
    }

    public void appendNodeToTail(ICodeNode node) {
        tailBlock.add(node);
    }

    public void appendNodesToTail(List<ICodeNode> nodes) {
        tailBlock.addAll(nodes);
    }

    public IdentifierCodeNode declareVariable(CodeNodeContext context, ICodeNode initialValue) {
        IdentifierCodeNode varName = context.createNewVariable();
        declareVariable(varName, initialValue);
        return varName;
    }

    public IdentifierCodeNode declareVariableAsFirstLine(CodeNodeContext context, ICodeNode initialValue) {
        IdentifierCodeNode varName = context.createNewVariable();
        blockVariables.put(varName, initialValue);
        headBlock.add(0, new AssignOperatorCodeNode(varName, initialValue));
        return varName;
    }

    public BlockCodeNode getParent() {
        return parent;
    }

    public LogicalOperatorTag getTag() {
        return tag;
    }

    void bindReader(IdentifierCodeNode reader) {
        boundReaders.put(reader, reader);
    }

    void bindReader(IdentifierCodeNode reader, IdentifierCodeNode array) {
        boundReaders.put(reader, array);
    }

    boolean isReaderBounded(IdentifierCodeNode variable) {
        return boundReaders.containsKey(variable);
    }

    boolean isReaderOverridden(IdentifierCodeNode variable) {
        boolean overridden = boundReaders.containsKey(variable) && boundReaders.get(variable) != variable;
        return overridden || parent != null && parent.isReaderOverridden(variable);
    }

    private void declareVariable(IdentifierCodeNode varName, ICodeNode initialValue) {
        blockVariables.put(varName, initialValue);
        appendNode(new AssignOperatorCodeNode(varName, initialValue));
    }

    @Override
    public <R, T> R accept(ICodeNodeVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visit(this, arg);
    }

    @Override
    public CodeNodeType getType() {
        return CodeNodeType.BLOCK;
    }

    public boolean isEmpty() {
        return headBlock.isEmpty() && tailBlock.isEmpty();
    }

    public IdentifierCodeNode getOverriddenReaderVariable(IdentifierCodeNode reader) {
        IdentifierCodeNode overriddenReader = boundReaders.get(reader);
        if (overriddenReader != null) {
            return overriddenReader;
        }
        return parent != null ? parent.getOverriddenReaderVariable(reader) : null;
    }
}
