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

import org.apache.hyracks.data.std.api.IValueReference;

public class NoOpColumnFilterWriter extends AbstractColumnFilterWriter {
    public static final AbstractColumnFilterWriter INSTANCE = new NoOpColumnFilterWriter();

    private NoOpColumnFilterWriter() {
    }

    @Override
    public void addLong(long value) {
        //NoOp
    }

    @Override
    public void addDouble(double value) {
        //NoOp
    }

    @Override
    public void addValue(IValueReference value) {
        //NoOp
    }

    @Override
    public long getMinNormalizedValue() {
        return -1;
    }

    @Override
    public long getMaxNormalizedValue() {
        return -1;
    }

    @Override
    public void reset() {
        //NoOp
    }

    @Override
    public void writeDecisive(OutputStream out) {
        //NoOp
    }
}
