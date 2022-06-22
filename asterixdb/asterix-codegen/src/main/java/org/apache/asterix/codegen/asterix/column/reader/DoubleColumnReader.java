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
import org.apache.asterix.codegen.truffle.runtime.array.storage.DoubleArrayStorage;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.om.types.ATypeTag;

public final class DoubleColumnReader extends AbstractTypedColumnReader {
    private double[] values;

    public DoubleColumnReader() {
        values = new double[INITIAL_SIZE];
    }

    public double getDouble() {
        return values[index];
    }

    @Override
    void setValue(IColumnValuesReader reader, int index) {
        values[index] = reader.getDouble();
    }

    @Override
    protected void expandValuesBuffer(int newLength) {
        values = Arrays.copyOf(values, newLength);
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.DOUBLE;
    }

    public double[] getValues() {
        return values;
    }

    @Override
    public AbstractArrayStorage createArray() {
        return new DoubleArrayStorage(false);
    }
}
