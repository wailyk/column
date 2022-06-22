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
package org.apache.asterix.codegen.truffle.runtime.aggregation.storage;

import static org.apache.asterix.codegen.asterix.map.UnsafeComparators.LONG_COMPARATOR;

import org.apache.asterix.codegen.asterix.map.IUnsafeHashAggregatorFactory;
import org.apache.asterix.codegen.asterix.map.UnsafeAggregators;
import org.apache.asterix.codegen.asterix.map.entry.LongEntry;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;
import org.apache.hyracks.unsafe.BytesToBytesMap.Location;
import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.spark.unsafe.Platform;

public class LongToLongHashAggregatorStorage extends AbstractHashAggregatorStorage {
    private final LongEntry mapKey;
    private final LongEntry mapValue;

    LongToLongHashAggregatorStorage(IUnsafeHashAggregatorFactory aggFactory, String aggType,
            AILResultWriter resultWriter) {
        super(aggFactory, resultWriter, UnsafeAggregators.getLongAggregator(aggType), LONG_COMPARATOR, LONG_COMPARATOR);
        mapKey = new LongEntry();
        mapValue = new LongEntry();
    }

    public void add(long key, long value) {
        mapKey.reset(key);
        mapValue.reset(value);
        aggregate(mapKey, mapValue);
    }

    @Override
    public void appendKey(AILResultWriter resultWriter, Location location) {
        resultWriter.append(Platform.getLong(location.getKeyBase(), location.getKeyOffset()));
    }

    @Override
    public void appendKey(AILResultWriter resultWriter, IEntry entry) {
        resultWriter.append(((LongEntry) entry).getValue());
    }

    @Override
    public void appendValue(AILResultWriter resultWriter, Location location) {
        resultWriter.append(Platform.getLong(location.getValueBase(), location.getValueOffset()));
    }

    @Override
    public void appendValue(AILResultWriter resultWriter, IEntry entry) {
        resultWriter.append(((LongEntry) entry).getValue());
    }

    @Override
    public HashTableType getType() {
        return HashTableType.LONG_LONG;
    }
}
