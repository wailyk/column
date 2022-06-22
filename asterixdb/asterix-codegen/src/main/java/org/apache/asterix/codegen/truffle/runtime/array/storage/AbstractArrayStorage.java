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

import org.apache.asterix.codegen.asterix.column.reader.AbstractTypedColumnReader;

public abstract class AbstractArrayStorage {
    private static final int INITIAL_CAPACITY = 32;
    private final boolean writeAsPairs;
    protected int length;
    protected int capacity;

    AbstractArrayStorage(boolean writeAsPairs) {
        this.length = 0;
        this.capacity = INITIAL_CAPACITY;
        this.writeAsPairs = writeAsPairs;
    }

    AbstractArrayStorage(AbstractTypedColumnReader reader) {
        setValues(reader);
        writeAsPairs = false;
    }

    public final int getLength() {
        return length;
    }

    public final int getCapacity() {
        return capacity;
    }

    public final void ensureCapacity(int newCapacity) {
        if (newCapacity > capacity) {
            increaseCapacityWithCopy(capacityFor(newCapacity));
        }
    }

    public final void reset() {
        length = 0;
    }

    public abstract ArrayType getStorageType();

    public abstract void sort();

    public abstract void asPairs(AbstractArrayStorage pairStorage);

    public abstract void distinct();

    public abstract AbstractArrayStorage createPairArray();

    public abstract void setValues(AbstractTypedColumnReader reader);

    protected abstract void increaseCapacityWithCopy(int newCapacity);

    /**
     * The capacity we should allocate for a given length.
     */
    private static int capacityFor(int length) throws ArithmeticException {
        return Math.max(16, Math.multiplyExact(length, 2));
    }
}
