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
package org.apache.asterix.codegen.asterix.column.reader;

import java.util.Arrays;

import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.codegen.truffle.runtime.array.storage.AbstractArrayStorage;
import org.apache.asterix.codegen.truffle.runtime.array.storage.StringArrayStorage;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public final class StringColumnReader extends AbstractTypedColumnReader {
    private final AILStringRuntime string;
    private ArrayBackedValueStorage[] values;

    public StringColumnReader() {
        string = new AILStringRuntime();
        values = new ArrayBackedValueStorage[INITIAL_SIZE];
        for (int i = 0; i < INITIAL_SIZE; i++) {
            values[i] = new ArrayBackedValueStorage(32);
        }
    }

    public AILStringRuntime getString() {
        string.reset(values[index]);
        return string;
    }

    @Override
    void setValue(IColumnValuesReader reader, int index) {
        values[index].set(reader.getBytes());
    }

    @Override
    protected void expandValuesBuffer(int newLength) {
        int oldLength = values.length;
        values = Arrays.copyOf(values, newLength);
        for (int i = oldLength; i < newLength; i++) {
            values[i] = new ArrayBackedValueStorage(32);
        }
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.STRING;
    }

    @Override
    public AbstractArrayStorage createArray() {
        return new StringArrayStorage(false);
    }
}
