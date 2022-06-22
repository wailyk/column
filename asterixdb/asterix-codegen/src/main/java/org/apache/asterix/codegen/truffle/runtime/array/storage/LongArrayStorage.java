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
import org.apache.asterix.codegen.asterix.column.reader.LongColumnReader;

public class LongArrayStorage extends AbstractArrayStorage {
    private long[] values;

    public LongArrayStorage(boolean writeAsPairs) {
        super(writeAsPairs);
    }

    public LongArrayStorage(AbstractTypedColumnReader reader) {
        super(reader);
    }

    @Override
    public void setValues(AbstractTypedColumnReader reader) {
        LongColumnReader longReader = (LongColumnReader) reader;
        values = longReader.getValues();
        length = reader.getNumberOfValues();
    }

    public long get(int index) {
        return values[index];
    }

    public void set(int index, long value) {
        values[index] = value;
    }

    public void append(long value) {
        ensureCapacity(length + 1);
        values[length++] = value;
    }

    @Override
    public void sort() {
        Arrays.sort(values, 0, length);
    }

    @Override
    public void asPairs(AbstractArrayStorage pairStorage) {
        LongArrayStorage longPairStorage = (LongArrayStorage) pairStorage;
        int localLength = length;

        longPairStorage.ensureCapacity(localLength * localLength);
        long[] pairValues = longPairStorage.values;
        int newLength = 0;

        long[] localValues = values;
        for (int i = 0; i < localLength; i++) {
            long iValue = localValues[i];
            for (int j = i + 1; j < localLength; j++) {
                pairValues[newLength++] = iValue;
                pairValues[newLength++] = localValues[j];
            }
        }
        longPairStorage.length = newLength;
    }

    @Override
    public void distinct() {
        sort();
        int nextDistinct = 0;
        int i = 1;
        while (i < length) {
            boolean areDifferent = values[nextDistinct] != values[i];
            if (areDifferent) {
                if (nextDistinct + 1 != i) {
                    values[nextDistinct] = values[i];
                }
                nextDistinct++;
            }
            i++;
        }
    }

    @Override
    public AbstractArrayStorage createPairArray() {
        return new LongArrayStorage(true);
    }

    @Override
    public ArrayType getStorageType() {
        return ArrayType.LONG;
    }

    @Override
    protected void increaseCapacityWithCopy(int newCapacity) {
        if (values == null) {
            values = new long[newCapacity];
        } else {
            values = Arrays.copyOf(values, newCapacity);
        }
        capacity = values.length;
    }

}
