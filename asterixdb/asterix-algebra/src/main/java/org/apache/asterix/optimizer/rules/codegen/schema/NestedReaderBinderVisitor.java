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
package org.apache.asterix.optimizer.rules.codegen.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.optimizer.rules.codegen.node.BlockCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeGenTemplates;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeContext;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.AbstractComplexExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ArrayExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNodeVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.ObjectExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.UnionExpectedSchemaNode;

/**
 * Binds a readers of a nested values
 * Example:
 * - Input: {"a": any, "b": any}
 * - Output:
 * reader1.hasNext --> for "a"
 * reader2.hasNext --> for "b"
 * <p>
 * Thus, the two reader1 and reader2 will be bound to the values "a" and "b", respectively.
 */
public class NestedReaderBinderVisitor implements IExpectedSchemaNodeVisitor<Void, CodeNodeContext> {
    private final List<ICodeNode> bindingNodes;
    private BlockCodeNode loopBlock;
    private IdentifierCodeNode firstReader;
    private IdentifierCodeNode indexVar;

    public NestedReaderBinderVisitor() {
        bindingNodes = new ArrayList<>();
    }

    public List<ICodeNode> bind(CodeNodeContext context, IExpectedSchemaNode node, BlockCodeNode loopBlock,
            IdentifierCodeNode indexVar) {
        bindingNodes.clear();
        firstReader = null;
        this.loopBlock = loopBlock;
        this.indexVar = indexVar;
        node.accept(this, context);
        return bindingNodes;
    }

    public IdentifierCodeNode getFirstReader() {
        return firstReader;
    }

    @Override
    public Void visit(RootExpectedSchemaNode node, CodeNodeContext arg) {
        //This an impossible case
        throw new IllegalStateException("Cannot bind root to a reader");
    }

    @Override
    public Void visit(ObjectExpectedSchemaNode node, CodeNodeContext arg) {
        for (Map.Entry<String, IExpectedSchemaNode> child : node.getChildren()) {
            child.getValue().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(ArrayExpectedSchemaNode node, CodeNodeContext arg) {
        node.getChild().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(UnionExpectedSchemaNode node, CodeNodeContext arg) {
        for (Map.Entry<ExpectedSchemaNodeType, AbstractComplexExpectedSchemaNode> child : node.getChildren()) {
            child.getValue().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(AnyExpectedSchemaNode node, CodeNodeContext context) {
        if (loopBlock != null || !context.isReaderBounded(node)) {
            if (indexVar == null) {
                bindingNodes.add(context.createNext(node, loopBlock));
            } else {
                bindingNodes.add(context.createToArray(node, indexVar, loopBlock));
            }

            if (loopBlock != null && context.isNestedScope()) {
                IdentifierCodeNode reader = context.getReader(node);
                ICodeNode rewindReader = CodeGenTemplates.createRewind(reader);
                context.getCurrentBlock().appendNode(rewindReader);
            }

        }

        if (firstReader == null) {
            firstReader = context.getReader(node);
        }
        return null;
    }
}
