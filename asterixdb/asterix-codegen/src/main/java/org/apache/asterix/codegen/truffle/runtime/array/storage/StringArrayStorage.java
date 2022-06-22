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
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class StringArrayStorage extends AbstractArrayStorage {
    private ArrayBackedValueStorage[] storages;
    private AILStringRuntime[] values;

    public StringArrayStorage(boolean writeAsPairs) {
        super(writeAsPairs);
    }

    public StringArrayStorage(AbstractTypedColumnReader reader) {
        super(reader);
    }

    @Override
    public void setValues(AbstractTypedColumnReader reader) {
        increaseCapacityWithCopy(reader.getNumberOfValues());
    }

    public AILStringRuntime get(int index) {
        return values[index];
    }

    public void set(int index, AILStringRuntime value) {
        ArrayBackedValueStorage valueStorage = storages[index];
        valueStorage.set(value.getStringValue());
        values[index].reset(valueStorage);
    }

    public void append(AILStringRuntime value) {
        ensureCapacity(length + 1);
        set(length, value);
        length++;
    }

    @Override
    public void sort() {
        Arrays.sort(values, 0, length, AILStringRuntime::compare);
    }

    @Override
    public void distinct() {
        sort();
        int nextDistinct = 0;
        int i = 1;
        while (i < length) {
            boolean areDifferent = !values[nextDistinct].isEqual(values[i]);
            if (areDifferent) {
                if (nextDistinct + 1 != i) {
                    values[nextDistinct].reset(values[i]);
                }
                nextDistinct++;
            }
            i++;
        }
    }

    @Override
    public AbstractArrayStorage createPairArray() {
        return new StringArrayStorage(true);
    }

    @Override
    public ArrayType getStorageType() {
        return ArrayType.DOUBLE;
    }

    @Override
    public void asPairs(AbstractArrayStorage pairStorage) {
        StringArrayStorage stringPairStorage = (StringArrayStorage) pairStorage;
        int localLength = length;

        stringPairStorage.ensureCapacity(localLength * localLength);
        ArrayBackedValueStorage[] pairStorages = stringPairStorage.storages;
        AILStringRuntime[] pairValues = stringPairStorage.values;

        int newLength = 0;

        ArrayBackedValueStorage[] localStorages = storages;
        for (int i = 0; i < localLength; i++) {
            ArrayBackedValueStorage iStorage = localStorages[i];
            for (int j = i + 1; j < localLength; j++) {
                ArrayBackedValueStorage jStorage = localStorages[j];

                ArrayBackedValueStorage firstStorage = pairStorages[newLength];
                firstStorage.set(iStorage);
                pairValues[newLength++].reset(firstStorage);

                ArrayBackedValueStorage secondStorage = pairStorages[newLength];
                firstStorage.set(jStorage);
                pairValues[newLength++].reset(secondStorage);
            }
        }
        stringPairStorage.length = newLength;
    }

    @Override
    protected void increaseCapacityWithCopy(int newCapacity) {
        if (values == null) {
            values = new AILStringRuntime[newCapacity];
            storages = new ArrayBackedValueStorage[newCapacity];
        } else {
            int oldLength = values.length;
            values = Arrays.copyOf(values, newCapacity);
            storages = Arrays.copyOf(storages, newCapacity);

            for (int i = oldLength; i < newCapacity; i++) {
                values[i] = new AILStringRuntime();
                storages[i] = new ArrayBackedValueStorage(32);
            }
        }

    }
}
