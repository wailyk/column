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
package org.apache.asterix.codegen.common;

import java.util.Random;

import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class DummyColumnReader implements IColumnValuesReader {
    private final AStringSerializerDeserializer stringSerDer;
    private final AMutableString mutableString;
    private final ArrayBackedValueStorage storage;
    private final int limit;
    private final Random random;
    private final ATypeTag typeTag;
    private int counter;
    private int value;

    public DummyColumnReader(ATypeTag typeTag, int limit) {
        stringSerDer = new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
        mutableString = new AMutableString("");
        storage = new ArrayBackedValueStorage();
        random = new Random(0);
        this.typeTag = typeTag;
        this.limit = limit;
    }

    @Override
    public void reset(AbstractBytesInputStream in, int tupleCount) throws HyracksDataException {
        //NoOp
    }

    @Override
    public boolean next() throws HyracksDataException {
        value = random.nextInt(100000);
        return counter++ < limit;
    }

    @Override
    public ATypeTag getTypeTag() {
        return typeTag;
    }

    @Override
    public int getColumnIndex() {
        return 0;
    }

    @Override
    public int getLevel() {
        return 0;
    }

    @Override
    public int getMaxLevel() {
        return 0;
    }

    @Override
    public boolean isMissing() {
        // 1/5 of the values are missing
        return value % 5 == 0;
    }

    @Override
    public boolean isNull() {
        // 1/5 of the values are nulls
        return value % 5 == 1;
    }

    @Override
    public boolean isValue() {
        // 3/5 of the values are present
        return value % 5 > 1;
    }

    @Override
    public boolean isRepeated() {
        return false;
    }

    @Override
    public boolean isDelimiter() {
        return false;
    }

    @Override
    public int getDelimiterIndex() {
        return 0;
    }

    @Override
    public boolean isLastDelimiter() {
        return false;
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    public double getDouble() {
        //Ensure the value is has a floating-point
        return value / 7.0;
    }

    @Override
    public boolean getBoolean() {
        return value % 2 == 0;
    }

    @Override
    public IValueReference getBytes() {
        mutableString.setValue(String.valueOf(value));
        try {
            stringSerDer.serialize(mutableString, storage.getDataOutput());
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
        return storage;
    }

    @Override
    public void write(IColumnValuesWriter writer, boolean callNext) throws HyracksDataException {

    }

    @Override
    public void write(IColumnValuesWriter writer, int count) throws HyracksDataException {

    }

    @Override
    public void skip(int count) throws HyracksDataException {

    }

    @Override
    public int compareTo(IColumnValuesReader o) {
        return 0;
    }
}
