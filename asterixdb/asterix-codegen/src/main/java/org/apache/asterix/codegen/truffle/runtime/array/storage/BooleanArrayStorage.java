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
package org.apache.asterix.codegen.truffle.runtime.array.storage;

import java.util.Arrays;

import org.apache.asterix.codegen.asterix.column.reader.AbstractTypedColumnReader;
import org.apache.asterix.codegen.asterix.column.reader.BooleanColumnReader;

public class BooleanArrayStorage extends AbstractArrayStorage {
    private boolean[] values;

    public BooleanArrayStorage(boolean writeAsPairs) {
        super(writeAsPairs);
    }

    public BooleanArrayStorage(AbstractTypedColumnReader reader) {
        super(reader);
    }

    @Override
    public void setValues(AbstractTypedColumnReader reader) {
        BooleanColumnReader booleanReader = (BooleanColumnReader) reader;
        values = booleanReader.getValues();
        length = reader.getNumberOfValues();
    }

    public boolean get(int index) {
        return values[index];
    }

    public void set(int index, boolean value) {
        values[index] = value;
    }

    public void append(boolean value) {
        ensureCapacity(length + 1);
        values[length++] = value;
    }

    @Override
    public void sort() {
        int counter = getNumberOfSetValues(values, length);

        for (int i = 0; i < length; i++) {
            values[i] = i < counter;
        }
    }

    @Override
    public void asPairs(AbstractArrayStorage pairStorage) {
        BooleanArrayStorage booleanPairStorage = (BooleanArrayStorage) pairStorage;
        int localLength = length;

        booleanPairStorage.ensureCapacity(localLength * localLength);
        boolean[] pairValues = booleanPairStorage.values;
        int newLength = 0;

        boolean[] localValues = values;
        for (int i = 0; i < localLength; i++) {
            boolean iValue = localValues[i];
            for (int j = i + 1; j < localLength; j++) {
                pairValues[newLength++] = iValue;
                pairValues[newLength++] = localValues[j];
            }
        }
        booleanPairStorage.length = newLength;
    }

    @Override
    public void distinct() {
        boolean previous = values[0];
        int i = 1;
        for (; previous == values[i] && i < length; i++) {
            previous = values[i];
        }
        if (i < length) {
            values[0] = true;
            values[1] = false;
            length = 2;
        } else {
            length = 1;
        }
    }

    @Override
    public AbstractArrayStorage createPairArray() {
        return new BooleanArrayStorage(true);
    }

    @Override
    public ArrayType getStorageType() {
        return ArrayType.BOOLEAN;
    }

    @Override
    protected void increaseCapacityWithCopy(int newCapacity) {
        if (values == null) {
            values = new boolean[newCapacity];
        } else {
            values = Arrays.copyOf(values, newCapacity);
        }
        capacity = values.length;
    }

    private static int getNumberOfSetValues(boolean[] values, int length) {
        int counter = 0;
        for (int i = 0; i < length; i++) {
            if (values[i]) {
                counter++;
            }
        }
        return counter;
    }

}
