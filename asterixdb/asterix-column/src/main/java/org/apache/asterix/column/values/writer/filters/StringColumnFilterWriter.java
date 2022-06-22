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
package org.apache.asterix.column.values.writer.filters;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class StringColumnFilterWriter extends AbstractColumnFilterWriter {
    private static final IBinaryComparator COMPARATOR =
            UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final ArrayBackedValueStorage min;
    private final ArrayBackedValueStorage max;
    private long minNorm;
    private long maxNorm;

    public StringColumnFilterWriter() {
        min = new ArrayBackedValueStorage();
        max = new ArrayBackedValueStorage();
    }

    @Override
    public void addValue(IValueReference value) throws HyracksDataException {
        if (min.getLength() == 0 || compare(min, value) > 0) {
            min.set(value);
            minNorm = normalize(value);
        }

        if (max.getLength() == 0 || compare(max, value) < 0) {
            max.set(value);
            maxNorm = normalize(value);
        }
    }

    @Override
    public long getMinNormalizedValue() {
        return minNorm;
    }

    @Override
    public long getMaxNormalizedValue() {
        return maxNorm;
    }

    public static long normalize(IValueReference value) {
        return UTF8StringUtil.normalize64(value.getByteArray(), value.getStartOffset());
    }

    @Override
    public void reset() {
        min.reset();
        max.reset();
    }

    @Override
    public void writeDecisive(OutputStream out) throws HyracksDataException {
        //        write(out, min);
        //        write(out, max);
    }

    //TODO compare only 8 bytes
    public static int compare(IValueReference v1, IValueReference v2) throws HyracksDataException {
        return COMPARATOR.compare(v1.getByteArray(), v1.getStartOffset(), v1.getLength(), v2.getByteArray(),
                v2.getStartOffset(), v2.getLength());
    }

    private void write(OutputStream out, ArrayBackedValueStorage value) throws HyracksDataException {
        try {
            out.write(value.getByteArray(), value.getStartOffset(), value.getLength());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
