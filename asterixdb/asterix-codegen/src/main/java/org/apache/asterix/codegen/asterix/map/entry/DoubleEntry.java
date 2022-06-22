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
package org.apache.asterix.codegen.asterix.map.entry;

import static org.apache.hyracks.unsafe.BytesToBytesMap.SEED;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.unsafe.BytesToBytesMap.Location;
import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;

public class DoubleEntry implements IEntry {
    private double value;

    public void reset(double value) {
        this.value = value;
    }

    @Override
    public boolean isEqual(Location location) {
        Object baseObject = location.getKeyBase();
        long offset = location.getKeyOffset();
        return Double.compare(value, Platform.getDouble(baseObject, offset)) == 0;
    }

    @Override
    public void setValue(Location location) {
        set(location.getValueBase(), location.getValueOffset(), -1);
    }

    @Override
    public void set(Object baseObject, long offset, long length) {
        Platform.putDouble(baseObject, offset, value);
    }

    @Override
    public void getValue(Location location) {
        get(location.getValueBase(), location.getValueOffset(), -1);
    }

    public double getValue() {
        return value;
    }

    @Override
    public void get(Object baseObject, long offset, long length) {
        value = Platform.getDouble(baseObject, offset);
    }

    @Override
    public int getLength() {
        return Double.BYTES;
    }

    @Override
    public int getHash() {
        return Murmur3_x86_32.hashLong(Double.doubleToLongBits(value), SEED);
    }

    @Override
    public byte getEntryTypeOrdinal() {
        return ATypeTag.DOUBLE.serialize();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        return compareTo((IEntry) o) == 0;
    }

    @Override
    public int hashCode() {
        return getHash();
    }

    @Override
    public int compareTo(IEntry o) {
        if (!(o instanceof DoubleEntry)) {
            return getEntryTypeOrdinal() - o.getEntryTypeOrdinal();
        }
        return Double.compare(value, ((DoubleEntry) o).value);
    }

    @Override
    public IEntry createCopy() {
        DoubleEntry copy = new DoubleEntry();
        copy.reset(value);
        return copy;
    }

    @Override
    public void reset(IEntry other) {
        reset(((DoubleEntry) other).value);
    }
}
