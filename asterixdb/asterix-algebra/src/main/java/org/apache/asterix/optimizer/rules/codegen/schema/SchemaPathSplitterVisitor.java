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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.pushdown.schema.AbstractComplexExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ArrayExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNodeVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.ObjectExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.UnionExpectedSchemaNode;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class SchemaPathSplitterVisitor implements IExpectedSchemaNodeVisitor<Void, Void> {
    private final Map<AnyExpectedSchemaNode, IAType> paths;
    private final Map<String, FunctionCallInformation> sourceInformationMap;
    private int nameCounter;

    public SchemaPathSplitterVisitor() {
        paths = new HashMap<>();
        sourceInformationMap = new HashMap<>();
    }

    public Map<AnyExpectedSchemaNode, IAType> split(IExpectedSchemaNode root) {
        paths.clear();
        sourceInformationMap.clear();
        root.accept(this, null);
        return paths;
    }

    public Map<String, FunctionCallInformation> getSourceInformationMap() {
        return sourceInformationMap;
    }

    @Override
    public Void visit(RootExpectedSchemaNode node, Void arg) {
        for (Map.Entry<String, IExpectedSchemaNode> child : node.getChildren()) {
            child.getValue().accept(this, null);
        }
        return null;
    }

    @Override
    public Void visit(ObjectExpectedSchemaNode node, Void arg) {
        for (Map.Entry<String, IExpectedSchemaNode> child : node.getChildren()) {
            child.getValue().accept(this, null);
        }
        return null;
    }

    @Override
    public Void visit(ArrayExpectedSchemaNode node, Void arg) {
        node.getChild().accept(this, null);
        return null;
    }

    @Override
    public Void visit(UnionExpectedSchemaNode node, Void arg) {
        for (Map.Entry<ExpectedSchemaNodeType, AbstractComplexExpectedSchemaNode> child : node.getChildren()) {
            child.getValue().accept(this, null);
        }
        return null;
    }

    @Override
    public Void visit(AnyExpectedSchemaNode node, Void arg) {
        //TODO add a reader for nested values requested entirely
        IExpectedSchemaNode nextNode = node;
        IAType nextType = BuiltinType.ANY;
        while (nextNode.getParent() != null) {
            AbstractComplexExpectedSchemaNode parent = nextNode.getParent();
            nextType = createType(parent, nextNode, nextType);
            nextNode = parent;
        }
        paths.put(node, nextType);
        return null;
    }

    private IAType createType(IExpectedSchemaNode parent, IExpectedSchemaNode childSchema, IAType childType) {
        String typeName = String.valueOf(nameCounter++);
        putFunctionCallInformation(typeName, parent);
        switch (parent.getType()) {
            case ARRAY:
                return new AOrderedListType(childType, typeName);
            case OBJECT:
                String fieldName = getFieldName(parent, childSchema);
                return new ARecordType(typeName, new String[] { fieldName }, new IAType[] { childType }, true);
            case UNION:
                return new AUnionType(List.of(childType), typeName);
            default:
                throw new IllegalStateException("ANY type is not a nested type");
        }
    }

    private String getFieldName(IExpectedSchemaNode parent, IExpectedSchemaNode child) {
        ObjectExpectedSchemaNode objectNode = (ObjectExpectedSchemaNode) parent;
        String fieldName = null;
        for (Map.Entry<String, IExpectedSchemaNode> kv : objectNode.getChildren()) {
            if (kv.getValue() == child) {
                fieldName = kv.getKey();
                break;
            }
        }
        return fieldName;
    }

    private void putFunctionCallInformation(String typeName, IExpectedSchemaNode node) {
        FunctionIdentifier fId = node.getFunctionIdentifier();
        if (fId == null) {
            return;
        }
        sourceInformationMap.put(typeName, new FunctionCallInformation(fId.getName(), node.getSourceLocation()));
    }
}
