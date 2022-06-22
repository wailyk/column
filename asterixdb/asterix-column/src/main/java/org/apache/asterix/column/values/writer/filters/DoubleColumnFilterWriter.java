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

import java.io.OutputStream;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class DoubleColumnFilterWriter extends AbstractColumnFilterWriter {
    private double min;
    private double max;

    public DoubleColumnFilterWriter() {
        reset();
    }

    @Override
    public void addDouble(double value) {
        if (min > value) {
            min = value;
        }

        if (max < value) {
            max = value;
        }
    }

    @Override
    public void addValue(IValueReference value) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMinNormalizedValue() {
        return normalize(min);
    }

    @Override
    public long getMaxNormalizedValue() {
        return normalize(max);
    }

    @Override
    public void reset() {
        min = Double.MIN_VALUE;
        max = Double.MAX_VALUE;
    }

    @Override
    public void writeDecisive(OutputStream out) {
        //NoOp
    }

    public static long normalize(double doubleValue) {
        long value = Double.doubleToLongBits(doubleValue);
        if (value >= 0) {
            value = value ^ Long.MIN_VALUE;
        } else {
            value = ~value;
        }
        return value;
    }
}
