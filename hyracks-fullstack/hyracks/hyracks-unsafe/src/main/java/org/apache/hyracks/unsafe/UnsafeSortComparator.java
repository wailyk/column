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
package org.apache.hyracks.unsafe;

import java.util.Comparator;

import org.apache.hyracks.unsafe.entry.IEntryComparator;
import org.apache.spark.unsafe.UnsafeAlignedOffset;

class UnsafeSortComparator implements Comparator<RecordPointerAndKeyPrefix> {
    private final IEntryComparator recordComparator;
    private final MemoryConsumer consumer;

    public UnsafeSortComparator(IEntryComparator recordComparator, MemoryConsumer consumer) {
        this.recordComparator = recordComparator;
        this.consumer = consumer;
    }

    @Override
    public int compare(RecordPointerAndKeyPrefix r1, RecordPointerAndKeyPrefix r2) {
        int prefixComparisonResult = recordComparator.comparePrefix(r1.keyPrefix, r2.keyPrefix);
        if (prefixComparisonResult == 0 && !recordComparator.isDecisive()) {
            int uaoSize = UnsafeAlignedOffset.getUaoSize();
            final Object baseObject1 = consumer.getPage(r1.recordPointer);
            final long baseOffset1 = consumer.getOffsetInPage(r1.recordPointer) + uaoSize * 2L;
            final int baseLength1 = UnsafeAlignedOffset.getSize(baseObject1, baseOffset1 - uaoSize);
            final Object baseObject2 = consumer.getPage(r2.recordPointer);
            final long baseOffset2 = consumer.getOffsetInPage(r2.recordPointer) + uaoSize * 2L;
            final int baseLength2 = UnsafeAlignedOffset.getSize(baseObject2, baseOffset2 - uaoSize);
            return recordComparator.compare(baseObject1, baseOffset1, baseLength1, baseObject2, baseOffset2,
                    baseLength2);
        }
        return prefixComparisonResult;
    }

    public long computePrefix(Object baseObject, long offset, int length) {
        return recordComparator.computePrefix(baseObject, offset, length);
    }
}
