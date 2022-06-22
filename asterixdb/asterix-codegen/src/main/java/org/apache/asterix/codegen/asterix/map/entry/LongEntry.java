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

public class LongEntry implements IEntry {
    private long value;

    public void reset(long value) {
        this.value = value;
    }

    @Override
    public boolean isEqual(Location location) {
        Object baseObject = location.getKeyBase();
        long offset = location.getKeyOffset();

        return value == Platform.getLong(baseObject, offset);
    }

    @Override
    public void setValue(Location location) {
        set(location.getValueBase(), location.getValueOffset(), -1);
    }

    @Override
    public void set(Object baseObject, long offset, long length) {
        Platform.putLong(baseObject, offset, value);
    }

    @Override
    public void get(Object baseObject, long offset, long length) {
        value = Platform.getLong(baseObject, offset);
    }

    @Override
    public void getValue(Location location) {
        get(location.getValueBase(), location.getValueOffset(), -1);
    }

    public long getValue() {
        return value;
    }

    @Override
    public int getLength() {
        return Long.BYTES;
    }

    @Override
    public int getHash() {
        return Murmur3_x86_32.hashLong(value, SEED);
    }

    @Override
    public byte getEntryTypeOrdinal() {
        return ATypeTag.BIGINT.serialize();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        LongEntry that = (LongEntry) o;
        return value == that.getValue();
    }

    @Override
    public int hashCode() {
        return getHash();
    }

    @Override
    public int compareTo(IEntry o) {
        if (!(o instanceof LongEntry)) {
            return getEntryTypeOrdinal() - o.getEntryTypeOrdinal();
        }
        return Long.compare(value, ((LongEntry) o).value);
    }

    @Override
    public IEntry createCopy() {
        LongEntry copy = new LongEntry();
        copy.reset(value);
        return copy;
    }

    @Override
    public void reset(IEntry other) {
        reset(((LongEntry) other).value);
    }
}
