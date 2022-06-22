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

import org.apache.asterix.codegen.truffle.runtime.array.storage.AbstractArrayStorage;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractTypedColumnReader {
    protected static final int INITIAL_SIZE = 64;
    private int numberOfValues;
    private boolean[] nulls;
    private boolean[] missings;

    protected int index;

    AbstractTypedColumnReader() {
        nulls = new boolean[INITIAL_SIZE];
        missings = new boolean[INITIAL_SIZE];
    }

    public final void setReader(IColumnValuesReader reader) throws HyracksDataException {
        boolean hasNext = reader.next() && reader.getMaxLevel() - reader.getLevel() <= 1;
        numberOfValues = 0;
        index = -1;
        while (hasNext) {
            ensureBuffers();
            if (reader.isMissing()) {
                missings[numberOfValues] = true;
            } else if (reader.isNull()) {
                nulls[numberOfValues] = true;
            } else {
                nulls[numberOfValues] = false;
                missings[numberOfValues] = false;
                setValue(reader, numberOfValues);
            }
            numberOfValues++;
            hasNext = reader.isRepeated() && reader.next() && !isEndOfArray(reader);
        }
    }

    public int getNumberOfValues() {
        return numberOfValues;
    }

    abstract void setValue(IColumnValuesReader reader, int index);

    private void ensureBuffers() {
        final int currentNumberOfValues = numberOfValues;
        if (nulls.length < currentNumberOfValues + 1) {
            final int newLength = currentNumberOfValues * 2;
            nulls = Arrays.copyOf(nulls, newLength);
            missings = Arrays.copyOf(missings, newLength);
            expandValuesBuffer(newLength);
        }
    }

    protected abstract void expandValuesBuffer(int newLength);

    public final void next() {
        index++;
    }

    public final void rewind() {
        index = 0;
    }

    public final boolean isEndOfArray() {
        return index >= numberOfValues;
    }

    private static boolean isEndOfArray(IColumnValuesReader reader) {
        return !reader.isRepeated() || reader.isLastDelimiter();
    }

    public final boolean isNull() {
        return nulls[index];
    }

    public final boolean isMissing() {
        return missings[index];
    }

    public abstract ATypeTag getTypeTag();

    public static AbstractTypedColumnReader createReader(ATypeTag typeTag) {
        switch (typeTag) {
            case BOOLEAN:
                return new BooleanColumnReader();
            case BIGINT:
                return new LongColumnReader();
            case DOUBLE:
                return new DoubleColumnReader();
            case STRING:
                return new StringColumnReader();
            case MISSING:
                return new MissingColumnReader();
            default:
                throw new IllegalStateException("not supported " + typeTag);
        }
    }

    public abstract AbstractArrayStorage createArray();
}
