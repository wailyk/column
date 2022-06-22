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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.column.metadata.AbstractColumnImmutableReadMetadata;
import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.UnsafeUtil;

public class QueryCodeGenerationColumnMetadata extends AbstractColumnImmutableReadMetadata {
    private final IColumnValuesReader[] primaryKeyReaders;
    private final IColumnValuesReader[] readers;

    protected QueryCodeGenerationColumnMetadata(ARecordType datasetType, IValueReference serializedMetadata,
            IColumnValuesReader[] primaryKeyReaders, IColumnValuesReader[] readers) {
        super(datasetType, null, primaryKeyReaders.length, serializedMetadata, readers.length);
        this.primaryKeyReaders = primaryKeyReaders;
        this.readers = readers;

    }

    @Override
    public AbstractColumnTupleReader createTupleReader() {
        return new QueryCodeGenerationColumnTupleReader(this);
    }

    public IColumnValuesReader[] getPrimaryKeyReaders() {
        return primaryKeyReaders;
    }

    public IColumnValuesReader[] getReaders() {
        return readers;
    }

    @Override
    public int getColumnIndex(int ordinal) {
        return readers[ordinal].getColumnIndex();
    }

    @Override
    public int getNumberOfProjectedColumns() {
        return readers.length;
    }

    /**
     * Create {@link QueryColumnMetadata} that would be used to determine the requested values
     *
     * @param datasetType         dataset declared type
     * @param numberOfPrimaryKeys number of PKs
     * @param serializedMetadata  inferred metadata (schema)
     * @param readerFactory       column reader factory
     * @param paths               requested paths
     * @param functionCallInfoMap function call information
     * @param warningCollector    warning collector
     * @return query metadata
     */
    public static QueryCodeGenerationColumnMetadata create(ARecordType datasetType, int numberOfPrimaryKeys,
            IValueReference serializedMetadata, IColumnValuesReaderFactory readerFactory, ARecordType[] paths,
            Map<String, FunctionCallInformation> functionCallInfoMap, IWarningCollector warningCollector)
            throws IOException {
        byte[] bytes = serializedMetadata.getByteArray();
        int offset = serializedMetadata.getStartOffset();
        int length = serializedMetadata.getLength();

        int fieldNamesStart = offset + UnsafeUtil.getInt(bytes, offset + FIELD_NAMES_POINTER);
        int metaRootStart = UnsafeUtil.getInt(bytes, offset + META_SCHEMA_POINTER);
        int metaRootSize = metaRootStart < 0 ? 0 : UnsafeUtil.getInt(bytes, offset + PATH_INFO_POINTER) - metaRootStart;
        DataInput input = new DataInputStream(new ByteArrayInputStream(bytes, fieldNamesStart, length));

        //FieldNames
        FieldNamesDictionary fieldNamesDictionary = FieldNamesDictionary.deserialize(input);

        //Schema
        ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, null);
        //Skip metaRoot (if exists)
        input.skipBytes(metaRootSize);

        //Clip schema
        PathReaderCreatorVisitor pathConverter = new PathReaderCreatorVisitor(fieldNamesDictionary, functionCallInfoMap,
                warningCollector, readerFactory);
        IColumnValuesReader[] columnPaths = createTruffleReaders(paths, root, pathConverter);

        IColumnValuesReader[] primaryKeyReaders = createPrimaryKeyReaders(input, readerFactory, numberOfPrimaryKeys);

        return new QueryCodeGenerationColumnMetadata(datasetType, serializedMetadata, primaryKeyReaders, columnPaths);
    }

    protected static IColumnValuesReader[] createTruffleReaders(ARecordType[] paths, ObjectSchemaNode root,
            PathReaderCreatorVisitor visitor) {
        IColumnValuesReader[] readers = new IColumnValuesReader[paths.length];
        for (int i = 0; i < paths.length; i++) {
            readers[i] = visitor.createReader(paths[i], root);
        }
        return readers;
    }

    protected static IColumnValuesReader[] createPrimaryKeyReaders(DataInput input,
            IColumnValuesReaderFactory readerFactory, int numberOfPrimaryKeys) throws IOException {
        //skip number of columns
        input.readInt();

        IColumnValuesReader[] primaryKeyReaders = new IColumnValuesReader[numberOfPrimaryKeys];
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            primaryKeyReaders[i] = readerFactory.createValueReader(input);
        }
        return primaryKeyReaders;
    }
}
