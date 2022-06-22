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

import org.apache.asterix.codegen.asterix.map.entry.DoubleEntry;
import org.apache.asterix.codegen.asterix.map.entry.LongEntry;
import org.apache.hyracks.unsafe.entry.IEntry;

public class UnsafeAggregators {
    public static final String MIN_NAME = "MIN";
    public static final String MAX_NAME = "MAX";
    private static final String SUM_NAME = "SUM";
    private static final String COUNT_NAME = "COUNT";

    public static AbstractLongAggregator getLongAggregator(String type) {
        switch (type) {
            case MIN_NAME:
                return LONG_MIN;
            case MAX_NAME:
                return LONG_MAX;
            case SUM_NAME:
                return LONG_SUM;
            case COUNT_NAME:
                return LONG_COUNT;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation " + type);
        }
    }

    public static AbstractDoubleAggregator getDoubleAggregator(String type) {
        switch (type) {
            case MIN_NAME:
                return DOUBLE_MIN;
            case MAX_NAME:
                return DOUBLE_MAX;
            case SUM_NAME:
                return DOUBLE_SUM;
            case COUNT_NAME:
                return DOUBLE_COUNT;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation " + type);
        }
    }

    /* ********************************************
     * Double Aggregators
     * ********************************************
     */

    private abstract static class AbstractDoubleAggregator implements IUnsafeAggregator {
        @Override
        public final IEntry createValueEntry() {
            return new LongEntry();
        }

        @Override
        public void initAggregateValue(IEntry aggregateEntry, IEntry newValue) {
            initAggregateDouble((DoubleEntry) aggregateEntry, (DoubleEntry) newValue);
        }

        @Override
        public final void aggregate(IEntry aggregateEntry, IEntry newValue) {
            aggregateDouble((DoubleEntry) aggregateEntry, (DoubleEntry) newValue);
        }

        protected abstract void initAggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue);

        protected abstract void aggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue);
    }

    private static final AbstractDoubleAggregator DOUBLE_MIN = new AbstractDoubleAggregator() {
        @Override
        protected void initAggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue) {
            aggregateEntry.reset(newValue.getValue());
        }

        @Override
        protected void aggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue) {
            aggregateEntry.reset(Math.min(aggregateEntry.getValue(), newValue.getValue()));
        }
    };

    private static final AbstractDoubleAggregator DOUBLE_MAX = new AbstractDoubleAggregator() {
        @Override
        protected void initAggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue) {
            aggregateEntry.reset(newValue.getValue());
        }

        @Override
        protected void aggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue) {
            aggregateEntry.reset(Math.max(aggregateEntry.getValue(), newValue.getValue()));
        }
    };

    private static final AbstractDoubleAggregator DOUBLE_SUM = new AbstractDoubleAggregator() {
        @Override
        protected void initAggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue) {
            aggregateEntry.reset(newValue.getValue());
        }

        @Override
        protected void aggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue) {
            aggregateEntry.reset(aggregateEntry.getValue() + newValue.getValue());
        }
    };

    private static final AbstractDoubleAggregator DOUBLE_COUNT = new AbstractDoubleAggregator() {
        @Override
        protected void initAggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue) {
            aggregateEntry.reset(1);
        }

        @Override
        protected void aggregateDouble(DoubleEntry aggregateEntry, DoubleEntry newValue) {
            aggregateEntry.reset(aggregateEntry.getValue() + 1);
        }
    };

    /* ********************************************
     * Long Aggregators
     * ********************************************
     */

    private abstract static class AbstractLongAggregator implements IUnsafeAggregator {
        @Override
        public final IEntry createValueEntry() {
            return new LongEntry();
        }

        @Override
        public void initAggregateValue(IEntry aggregateEntry, IEntry newValue) {
            initAggregateLong((LongEntry) aggregateEntry, (LongEntry) newValue);
        }

        @Override
        public final void aggregate(IEntry aggregateEntry, IEntry newValue) {
            aggregateLong((LongEntry) aggregateEntry, (LongEntry) newValue);
        }

        protected abstract void initAggregateLong(LongEntry aggregateEntry, LongEntry newValue);

        protected abstract void aggregateLong(LongEntry aggregateEntry, LongEntry newValue);
    }

    private static final AbstractLongAggregator LONG_MIN = new AbstractLongAggregator() {
        @Override
        protected void initAggregateLong(LongEntry aggregateEntry, LongEntry newValue) {
            aggregateEntry.reset(newValue.getValue());
        }

        @Override
        protected void aggregateLong(LongEntry aggregateEntry, LongEntry newValue) {
            aggregateEntry.reset(Math.min(aggregateEntry.getValue(), newValue.getValue()));
        }
    };

    private static final AbstractLongAggregator LONG_MAX = new AbstractLongAggregator() {
        @Override
        protected void initAggregateLong(LongEntry aggregateEntry, LongEntry newValue) {
            aggregateEntry.reset(newValue.getValue());
        }

        @Override
        protected void aggregateLong(LongEntry aggregateEntry, LongEntry newValue) {
            aggregateEntry.reset(Math.max(aggregateEntry.getValue(), newValue.getValue()));
        }
    };

    private static final AbstractLongAggregator LONG_SUM = new AbstractLongAggregator() {
        @Override
        protected void initAggregateLong(LongEntry aggregateEntry, LongEntry newValue) {
            aggregateEntry.reset(newValue.getValue());
        }

        @Override
        protected void aggregateLong(LongEntry aggregateEntry, LongEntry newValue) {
            aggregateEntry.reset(aggregateEntry.getValue() + newValue.getValue());
        }
    };

    private static final AbstractLongAggregator LONG_COUNT = new AbstractLongAggregator() {
        @Override
        protected void initAggregateLong(LongEntry aggregateEntry, LongEntry newValue) {
            aggregateEntry.reset(1);
        }

        @Override
        protected void aggregateLong(LongEntry aggregateEntry, LongEntry newValue) {
            aggregateEntry.reset(aggregateEntry.getValue() + 1);
        }
    };
}
