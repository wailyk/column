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

import org.apache.asterix.codegen.asterix.map.entry.StringEntryUtil;
import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.hyracks.unsafe.entry.IEntryComparator;
import org.apache.spark.unsafe.Platform;

public class UnsafeComparators {
    private UnsafeComparators() {
    }

    public static final IEntryComparator LONG_COMPARATOR = new IEntryComparator() {
        @Override
        public int compare(Object leftBaseObject, long leftBaseOffset, int leftBaseLength, Object rightBaseObject,
                long rightBaseOffset, int rightBaseLength) {
            return Long.compare(Platform.getLong(leftBaseObject, leftBaseOffset),
                    Platform.getLong(rightBaseObject, rightBaseOffset));
        }

        @Override
        public long computePrefix(Object basedObject, long offset, int length) {
            return Platform.getLong(basedObject, offset);
        }

        @Override
        public int compare(IEntry left, IEntry right) {
            return left.compareTo(right);
        }

        @Override
        public int comparePrefix(long left, long right) {
            return Long.compare(left, right);
        }

        @Override
        public boolean isDecisive() {
            return true;
        }
    };

    public static final IEntryComparator DOUBLE_COMPARATOR = new IEntryComparator() {
        @Override
        public int compare(Object leftBaseObject, long leftBaseOffset, int leftBaseLength, Object rightBaseObject,
                long rightBaseOffset, int rightBaseLength) {
            return Double.compare(Platform.getDouble(leftBaseObject, leftBaseOffset),
                    Platform.getDouble(rightBaseObject, rightBaseOffset));
        }

        @Override
        public int compare(IEntry left, IEntry right) {
            return left.compareTo(right);
        }

        @Override
        public long computePrefix(Object basedObject, long offset, int length) {
            return Double.doubleToLongBits(Platform.getDouble(basedObject, offset));
        }

        @Override
        public int comparePrefix(long left, long right) {
            return Double.compare(Double.longBitsToDouble(left), Double.longBitsToDouble(right));
        }

        @Override
        public boolean isDecisive() {
            return true;
        }
    };

    public static final IEntryComparator STRING_COMPARATOR = new IEntryComparator() {
        @Override
        public int compare(Object leftBaseObject, long leftBaseOffset, int leftBaseLength, Object rightBaseObject,
                long rightBaseOffset, int rightBaseLength) {
            return StringEntryUtil.compare(leftBaseObject, leftBaseOffset, leftBaseLength, rightBaseObject,
                    rightBaseOffset, rightBaseLength);
        }

        @Override
        public int compare(IEntry left, IEntry right) {
            return left.compareTo(right);
        }

        @Override
        public long computePrefix(Object basedObject, long offset, int length) {
            return StringEntryUtil.computePrefix(basedObject, offset, length);
        }

        @Override
        public int comparePrefix(long left, long right) {
            return Long.compare(left, right);
        }

        @Override
        public boolean isDecisive() {
            return false;
        }
    };
}
