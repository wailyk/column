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
package org.apache.asterix.column.operation.query;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;

public class SchemaClipperVisitor implements IATypeVisitor<AbstractSchemaNode, AbstractSchemaNode> {
    protected final FieldNamesDictionary fieldNamesDictionary;
    protected final IWarningCollector warningCollector;
    protected final Map<String, FunctionCallInformation> functionCallInfoMap;

    public SchemaClipperVisitor(FieldNamesDictionary fieldNamesDictionary,
            Map<String, FunctionCallInformation> functionCallInfoMap, IWarningCollector warningCollector) {
        this.fieldNamesDictionary = fieldNamesDictionary;
        this.functionCallInfoMap = functionCallInfoMap;
        this.warningCollector = warningCollector;
    }

    @Override
    public AbstractSchemaNode visit(ARecordType recordType, AbstractSchemaNode arg) {
        if (isNotCompatible(recordType, arg)) {
            return MissingFieldSchemaNode.INSTANCE;
        }

        String[] fieldNames = recordType.getFieldNames();
        IAType[] fieldTypes = recordType.getFieldTypes();
        ObjectSchemaNode objectNode = getActualNode(arg, ATypeTag.OBJECT, ObjectSchemaNode.class);

        ObjectSchemaNode clippedObjectNode = new ObjectSchemaNode();
        try {
            for (int i = 0; i < fieldNames.length; i++) {
                int fieldNameIndex = fieldNamesDictionary.getFieldNameIndex(fieldNames[i]);
                AbstractSchemaNode child = objectNode.getChild(fieldNameIndex);
                clippedObjectNode.addChild(fieldNameIndex, fieldTypes[i].accept(this, child));
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        return clippedObjectNode;
    }

    @Override
    public AbstractSchemaNode visit(AbstractCollectionType collectionType, AbstractSchemaNode arg) {
        if (isNotCompatible(collectionType, arg)) {
            return MissingFieldSchemaNode.INSTANCE;
        }
        AbstractCollectionSchemaNode collectionNode =
                getActualNode(arg, collectionType.getTypeTag(), AbstractCollectionSchemaNode.class);
        AbstractSchemaNode newItemNode = collectionType.getItemType().accept(this, collectionNode.getItemNode());
        collectionNode.setItemNode(newItemNode);
        return collectionNode;
    }

    @Override
    public AbstractSchemaNode visit(AUnionType unionType, AbstractSchemaNode arg) {
        return arg;
    }

    @Override
    public AbstractSchemaNode visitFlat(IAType flatType, AbstractSchemaNode arg) {
        return arg;
    }

    protected <T extends AbstractSchemaNode> T getActualNode(AbstractSchemaNode node, ATypeTag typeTag,
            Class<T> clazz) {
        if (node.getTypeTag() == typeTag) {
            return clazz.cast(node);
        } else {
            //Then it is a union
            UnionSchemaNode unionNode = (UnionSchemaNode) node;
            return clazz.cast(unionNode.getChild(typeTag));
        }
    }

    protected boolean isNotCompatible(IAType requestedType, AbstractSchemaNode schemaNode) {
        if (requestedType.getTypeTag() != schemaNode.getTypeTag() && (schemaNode.getTypeTag() != ATypeTag.UNION
                || !((UnionSchemaNode) schemaNode).getChildren().containsKey(requestedType.getTypeTag()))) {
            if (warningCollector.shouldWarn()) {
                Warning warning = functionCallInfoMap.get(requestedType.getTypeName())
                        .createTypeMismatchWarning(requestedType.getTypeTag(), schemaNode.getTypeTag());
                if (warning != null) {
                    warningCollector.warn(warning);
                }
            }
            return true;
        }
        return false;
    }
}
