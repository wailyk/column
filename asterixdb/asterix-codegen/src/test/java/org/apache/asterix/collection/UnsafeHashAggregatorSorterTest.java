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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.asterix.codegen.asterix.map.UnsafeAggregators;
import org.apache.asterix.codegen.asterix.map.UnsafeComparators;
import org.apache.asterix.codegen.asterix.map.UnsafeHashAggregator;
import org.apache.asterix.codegen.asterix.map.entry.LongEntry;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.unsafe.BytesToBytesMap.Location;
import org.junit.Assert;
import org.junit.Test;

public class UnsafeHashAggregatorSorterTest {
    private static final long BUDGET = 8 << 20;
    private static final int SIZE = 100000;

    @Test
    public void test() throws HyracksDataException {
        UnsafeHashAggregator computer = new UnsafeHashAggregator(UnsafeAggregators.getLongAggregator("COUNT"), null,
                UnsafeComparators.LONG_COMPARATOR, BUDGET);
        Random random = new Random(0);
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        AILStringRuntime string = new AILStringRuntime();

        LongEntry key = new LongEntry();
        LongEntry value = new LongEntry();
        Set<Long> values = new HashSet<>();

        for (int i = 0; i < SIZE; i++) {
            long longValue = random.nextInt(1000000);

            key.reset(longValue);
            value.reset(longValue);
            computer.aggregate(key, value);

            values.add(longValue);
        }
        Assert.assertEquals(values.size(), computer.size());
        Long[] valuesList = values.toArray(new Long[0]);
        Arrays.sort(valuesList);
        Iterator<Location> iter = computer.sortedIterator();
        int i = 0;
        while (iter.hasNext()) {
            Location location = iter.next();
            Object baseObject = location.getKeyBase();
            long offset = location.getKeyOffset();
            int length = location.getKeyLength();
            key.get(baseObject, offset, length);
            Assert.assertEquals((long) valuesList[i++], key.getValue());
        }
    }
}
