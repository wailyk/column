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
package org.apache.asterix.collection;

import java.nio.charset.StandardCharsets;

import org.apache.asterix.codegen.asterix.map.entry.DoubleEntry;
import org.apache.asterix.codegen.asterix.map.entry.LongEntry;
import org.apache.asterix.codegen.asterix.map.entry.StringEntry;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.unsafe.BytesToBytesMap;
import org.apache.hyracks.unsafe.BytesToBytesMap.Location;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.junit.Assert;
import org.junit.Test;

public class BytesToBytesMapTest {

    @Test
    public void testBytes() {
        BytesToBytesMap map = new BytesToBytesMap(MemoryAllocator.HEAP, 64 << 20, 1 << 20, null);
        StringEntry someKey = createBytesEntry("fjqjkfkjreqgkjerbgkjfhrelkgjhlk");
        StringEntry someValue = createBytesEntry(",mdcnkncnnu8euhfe;fj");
        Location location = map.lookup(someKey);
        assert !location.isDefined();
        location.append(someKey, someValue);

        Location locAfterPut = map.lookup(someKey);
        assert locAfterPut.isDefined();

        //Reset someValue to its original size
        byte[] someValueBytes = someValue.getValue().getByteArray();
        int originalLength = VarLenIntEncoderDecoder.decode(someValueBytes, 0);
        int sizeByteLength = VarLenIntEncoderDecoder.getBytesRequired(originalLength);
        VoidPointable someValueWithOriginalSize = new VoidPointable();
        someValueWithOriginalSize.set(someValueBytes, 0, originalLength + sizeByteLength);

        StringEntry putValue = new StringEntry();
        putValue.getValue(location);
        Assert.assertTrue(UTF8StringPointable.areEqual(someValueWithOriginalSize, putValue.getValue()));
    }

    @Test
    public void testLong() {
        BytesToBytesMap map = new BytesToBytesMap(MemoryAllocator.HEAP, 64 << 20, 1 << 20, null);
        LongEntry someKey = new LongEntry();
        someKey.reset(101010);
        LongEntry someValue = new LongEntry();
        someValue.reset(123456);

        Location location = map.lookup(someKey);
        assert !location.isDefined();
        location.append(someKey, someValue);

        Location locAfterPut = map.lookup(someKey);
        assert locAfterPut.isDefined();

        LongEntry putValue = new LongEntry();
        putValue.getValue(location);
        Assert.assertEquals(someValue.getValue(), putValue.getValue());
    }

    @Test
    public void testDouble() {
        BytesToBytesMap map = new BytesToBytesMap(MemoryAllocator.HEAP, 64 << 20, 1 << 20, null);
        DoubleEntry someKey = new DoubleEntry();
        someKey.reset(101010.0D);
        DoubleEntry someValue = new DoubleEntry();
        someValue.reset(123456.0D);

        Location location = map.lookup(someKey);
        assert !location.isDefined();
        location.append(someKey, someValue);

        Location locAfterPut = map.lookup(someKey);
        assert locAfterPut.isDefined();

        DoubleEntry putValue = new DoubleEntry();
        putValue.getValue(location);
        Assert.assertEquals(0, Double.compare(someValue.getValue(), putValue.getValue()));
    }

    private static StringEntry createBytesEntry(String value) {
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        int valueStart = VarLenIntEncoderDecoder.getBytesRequired(valueBytes.length);
        byte[] storageByte = new byte[valueStart + valueBytes.length];
        VarLenIntEncoderDecoder.encode(valueBytes.length, storageByte, 0);
        System.arraycopy(valueBytes, 0, storageByte, valueStart, valueBytes.length);
        VoidPointable pointable = new VoidPointable();
        pointable.set(storageByte, 0, storageByte.length);
        StringEntry entry = new StringEntry();
        entry.reset(pointable);
        return entry;
    }

}
