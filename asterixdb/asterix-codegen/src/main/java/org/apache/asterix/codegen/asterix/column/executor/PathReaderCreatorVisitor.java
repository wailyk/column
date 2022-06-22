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
package org.apache.asterix.codegen.asterix.column.executor;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.operation.query.SchemaClipperVisitor;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.IWarningCollector;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class PathReaderCreatorVisitor extends SchemaClipperVisitor {
    private final IColumnValuesReaderFactory readerFactory;
    private final IntList delimiters;
    private int level = 0;

    public PathReaderCreatorVisitor(FieldNamesDictionary fieldNamesDictionary,
            Map<String, FunctionCallInformation> functionCallInfoMap, IWarningCollector warningCollector,
            IColumnValuesReaderFactory readerFactory) {
        super(fieldNamesDictionary, functionCallInfoMap, warningCollector);
        this.readerFactory = readerFactory;
        delimiters = new IntArrayList();
    }

    public IColumnValuesReader createReader(ARecordType path, AbstractSchemaNode root) {
        delimiters.clear();
        level = 0;
        AbstractSchemaNode node = path.accept(this, root);
        if (node.isNested()) {
            throw new UnsupportedOperationException("Nested values are not supported with code generation");
        }
        PrimitiveSchemaNode primitiveNode = (PrimitiveSchemaNode) node;
        if (delimiters.isEmpty()) {
            return readerFactory.createValueReader(primitiveNode.getTypeTag(), primitiveNode.getColumnIndex(), level,
                    primitiveNode.isPrimaryKey());
        }
        return readerFactory.createValueReader(primitiveNode.getTypeTag(), primitiveNode.getColumnIndex(), level,
                getDelimiters());
    }

    @Override
    public AbstractSchemaNode visit(ARecordType recordType, AbstractSchemaNode arg) {
        if (isNotCompatible(recordType, arg)) {
            return MissingFieldSchemaNode.INSTANCE;
        }
        level++;
        //There is only one field per path
        String fieldName = recordType.getFieldNames()[0];
        IAType fieldType = recordType.getFieldTypes()[0];
        ObjectSchemaNode objectNode = getActualNode(arg, ATypeTag.OBJECT, ObjectSchemaNode.class);

        try {
            int fieldNameIndex = fieldNamesDictionary.getFieldNameIndex(fieldName);
            return fieldType.accept(this, objectNode.getChild(fieldNameIndex));

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }

    @Override
    public AbstractSchemaNode visit(AbstractCollectionType collectionType, AbstractSchemaNode arg) {
        if (isNotCompatible(collectionType, arg)) {
            return MissingFieldSchemaNode.INSTANCE;
        }
        delimiters.add(level - 1);
        level++;
        AbstractCollectionSchemaNode collectionNode =
                getActualNode(arg, collectionType.getTypeTag(), AbstractCollectionSchemaNode.class);
        return collectionType.getItemType().accept(this, collectionNode.getItemNode());
    }

    private int[] getDelimiters() {
        int numOfDelimiters = delimiters.size();
        int[] reversed = new int[numOfDelimiters];
        for (int i = 0; i < numOfDelimiters; i++) {
            reversed[i] = delimiters.getInt(numOfDelimiters - i - 1);
        }
        return reversed;
    }
}
