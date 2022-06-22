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
package org.apache.asterix.codegen.asterix.map;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.codegen.asterix.map.entry.IUnsafeMapResultAppender;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;
import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.hyracks.unsafe.entry.IEntryComparator;

public class UnsafeHashMaxTopKAggregator extends AbstractUnsafeHashAggregator {
    private final Map<IEntry, IEntry> map;
    private final IEntryComparator comparator;
    private IEntry minKey;
    private final int k;

    public UnsafeHashMaxTopKAggregator(IUnsafeAggregator aggregator, IUnsafeMapResultAppender appender,
            IEntryComparator comparator, int k) {
        super(aggregator, appender);
        map = new HashMap<>();
        this.comparator = comparator;
        this.k = k;
    }

    @Override
    public boolean aggregate(IEntry key, IEntry value) {
        IEntry aggValue = map.get(key);
        if (aggValue != null) {
            aggregator.aggregate(aggValue, value);
            if (minKey.equals(key)) {
                //They're the same -- find the new minimum
                findNewMinKey();
            }
        } else if (map.size() < k) {
            IEntry keyCopy = key.createCopy();
            IEntry valueCopy = value.createCopy();
            aggregator.initAggregateValue(valueCopy, value);
            map.put(keyCopy, valueCopy);
            //Set the new min/max aggregate's key
            if (minKey == null || greaterThan(minKey, valueCopy)) {
                minKey = keyCopy;
            }
        } else if (lessThan(minKey, value)) {
            IEntry oldValue = map.remove(minKey);
            aggregator.initAggregateValue(oldValue, value);
            minKey.reset(key);
            map.put(minKey, oldValue);
            findNewMinKey();
        }
        //Ignore aggregate as it is not in the top K

        //Always return true as currently we will not go over budget
        return true;
    }

    private boolean greaterThan(IEntry leftKey, IEntry rightValue) {
        IEntry leftValue = map.get(leftKey);
        return comparator.compare(leftValue, rightValue) > 0;
    }

    private boolean lessThan(IEntry leftKey, IEntry rightValue) {
        IEntry leftValue = map.get(leftKey);
        return comparator.compare(leftValue, rightValue) < 0;
    }

    @Override
    public void append(AILResultWriter resultWriter) {
        map.forEach((key, value) -> {
            appender.appendKey(resultWriter, key);
            appender.appendValue(resultWriter, value);
            resultWriter.flush();
        });
        map.clear();
    }

    private void findNewMinKey() {
        IEntry currentMinValue = null;
        IEntry currentMinKey = null;
        for (Map.Entry<IEntry, IEntry> kv : map.entrySet()) {
            if (currentMinValue == null || greaterThan(currentMinKey, kv.getValue())) {
                currentMinKey = kv.getKey();
                currentMinValue = map.get(currentMinKey);
            }
        }
        minKey = currentMinKey;
    }
}
