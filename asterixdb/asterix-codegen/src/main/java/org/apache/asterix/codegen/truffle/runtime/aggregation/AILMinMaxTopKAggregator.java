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
package org.apache.asterix.codegen.truffle.runtime.aggregation;

import org.apache.asterix.codegen.asterix.map.AbstractUnsafeHashAggregator;
import org.apache.asterix.codegen.asterix.map.IUnsafeAggregator;
import org.apache.asterix.codegen.asterix.map.IUnsafeHashAggregatorFactory;
import org.apache.asterix.codegen.asterix.map.UnsafeAggregators;
import org.apache.asterix.codegen.asterix.map.UnsafeHashMaxTopKAggregator;
import org.apache.asterix.codegen.asterix.map.entry.IUnsafeMapResultAppender;
import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.hyracks.unsafe.entry.IEntryComparator;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

@ExportLibrary(InteropLibrary.class)
public class AILMinMaxTopKAggregator extends AbstractAggregator implements TruffleObject {

    public AILMinMaxTopKAggregator(int k, boolean min) {
        super(new MinMaxTopKAggregator(k, min), min ? UnsafeAggregators.MIN_NAME : UnsafeAggregators.MAX_NAME);
    }

    @ExportMessage
    Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects) {
        return "TopK Computer";
    }

    @ExportMessage
    boolean hasLanguage() {
        return true;
    }

    @ExportMessage
    Class<? extends TruffleLanguage<?>> getLanguage() {
        return AILLanguage.class;
    }

    private static class MinMaxTopKAggregator implements IUnsafeHashAggregatorFactory {
        private final int k;
        private final boolean min;

        MinMaxTopKAggregator(int k, boolean min) {
            this.k = k;
            this.min = min;
        }

        @Override
        public AbstractUnsafeHashAggregator createInstance(IUnsafeAggregator aggregator,
                IUnsafeMapResultAppender appender, IEntryComparator keyComparator, IEntryComparator valueComparator) {
            IEntryComparator minMaxComparator = min ? new MinEntryComparator(valueComparator) : valueComparator;
            return new UnsafeHashMaxTopKAggregator(aggregator, appender, minMaxComparator, k);
        }
    }

    private static class MinEntryComparator implements IEntryComparator {
        private final IEntryComparator comparator;

        MinEntryComparator(IEntryComparator comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Object leftBaseObject, long leftBaseOffset, int leftBaseLength, Object rightBaseObject,
                long rightBaseOffset, int rightBaseLength) {
            return -comparator.compare(leftBaseObject, leftBaseOffset, leftBaseLength, rightBaseObject, rightBaseOffset,
                    rightBaseLength);
        }

        @Override
        public int compare(IEntry left, IEntry right) {
            return -comparator.compare(left, right);
        }

        @Override
        public long computePrefix(Object basedObject, long offset, int length) {
            return comparator.computePrefix(basedObject, offset, length);
        }

        @Override
        public int comparePrefix(long left, long right) {
            return -comparator.comparePrefix(left, right);
        }

        @Override
        public boolean isDecisive() {
            return comparator.isDecisive();
        }
    }
}
