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
import org.apache.asterix.codegen.asterix.column.reader.DoubleColumnReader;

public class DoubleArrayStorage extends AbstractArrayStorage {
    private double[] values;

    public DoubleArrayStorage(boolean writeAsPairs) {
        super(writeAsPairs);
    }

    public DoubleArrayStorage(AbstractTypedColumnReader reader) {
        super(reader);
    }

    @Override
    public void setValues(AbstractTypedColumnReader reader) {
        DoubleColumnReader doubleReader = (DoubleColumnReader) reader;
        values = doubleReader.getValues();
        length = doubleReader.getNumberOfValues();
    }

    public double get(int index) {
        if (index >= length) {
            throw new ArrayIndexOutOfBoundsException("index: " + index + " length: " + length);
        }
        return values[index];
    }

    public void set(int index, double value) {
        values[index] = value;
    }

    public void append(double value) {
        ensureCapacity(length + 1);
        values[length++] = value;
    }

    @Override
    public void sort() {
        Arrays.sort(values, 0, length);
    }

    @Override
    public void asPairs(AbstractArrayStorage pairStorage) {
        DoubleArrayStorage doublePairStorage = (DoubleArrayStorage) pairStorage;
        int localLength = length;

        doublePairStorage.ensureCapacity(localLength * localLength);
        double[] pairValues = doublePairStorage.values;
        int newLength = 0;

        double[] localValues = values;
        for (int i = 0; i < localLength; i++) {
            double iValue = localValues[i];
            for (int j = i + 1; j < localLength; j++) {
                pairValues[newLength++] = iValue;
                pairValues[newLength++] = localValues[j];
            }
        }
        doublePairStorage.length = newLength;
    }

    @Override
    public void distinct() {
        sort();
        int nextDistinct = 0;
        int i = 1;
        while (i < length) {
            boolean areDifferent = Double.compare(values[nextDistinct], values[i]) != 0;
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
        return new DoubleArrayStorage(true);
    }

    @Override
    public ArrayType getStorageType() {
        return ArrayType.DOUBLE;
    }

    @Override
    protected void increaseCapacityWithCopy(int newCapacity) {
        if (values == null) {
            values = new double[newCapacity];
        } else {
            values = Arrays.copyOf(values, newCapacity);
        }
        capacity = values.length;
    }
}
