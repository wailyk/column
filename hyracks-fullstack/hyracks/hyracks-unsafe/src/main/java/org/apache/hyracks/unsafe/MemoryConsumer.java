/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hyracks.unsafe;

import java.util.BitSet;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * Repurposed from Apache Spark
 * <a href="https://github.com/apache/spark/blob/6b5a1f9df28262fa90d28dc15af67e8a37a9efcf/core/src/main/java/org/apache/spark/unsafe/map/BytesToBytesMap.java"></a>
 */
abstract class MemoryConsumer {
    static final long PAGE_SIZE = 1 << 20;
    /** The number of bits used to address the page table. */
    private static final int PAGE_NUMBER_BITS = 13;
    /** The number of bits used to encode offsets in data pages. */
    static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS; // 51
    /** Bit mask for the lower 51 bits of a long. */
    private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;
    /** The number of entries in the page table. */
    private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

    private final long budget;
    private final MemoryAllocator allocator;
    private final BitSet allocatedPages;
    private final MemoryBlock[] pageTable;
    protected long used;

    protected MemoryConsumer(MemoryAllocator allocator, long budget) {
        this.allocator = allocator;
        this.budget = roundedBudget(budget);
        allocatedPages = new BitSet(PAGE_TABLE_SIZE);
        pageTable = new MemoryBlock[PAGE_TABLE_SIZE];
        used = 0;
    }

    private static long roundedBudget(long budget) {
        return budget + (budget % PAGE_SIZE + PAGE_SIZE);
    }

    boolean canAllocateArray(int size, long oldSize) {
        long required = size * 8L;
        return used - oldSize + required <= budget;
    }

    LongArray allocateArray(long size, long oldSize) {
        long required = size * 8L;
        MemoryBlock page = allocate(required, oldSize * 8L);
        if (page != null) {
            return new LongArray(page);
        }
        return null;
    }

    /**
     * Frees a LongArray.
     */
    void freeArray(LongArray array) {
        freePage(array.memoryBlock());
    }

    private MemoryBlock allocate(long size, long oldSize) {
        final int pageNumber;
        pageNumber = allocatedPages.nextClearBit(0);
        if (pageNumber >= PAGE_TABLE_SIZE || used - oldSize + size > budget) {
            return null;
        }
        allocatedPages.set(pageNumber);
        MemoryBlock page = allocator.allocate(size);
        page.pageNumber = pageNumber;
        pageTable[pageNumber] = page;
        used += page.size();
        return page;
    }

    MemoryBlock allocatePage(long size) {
        return allocate(Math.max(size, PAGE_SIZE), 0);
    }

    /**
     * Free a memory block.
     */
    void freePage(MemoryBlock page) {
        allocatedPages.clear(page.pageNumber);
        pageTable[page.pageNumber] = null;
        page.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;

        used -= page.size();
        allocator.free(page);
    }

    protected long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
        if (allocator == MemoryAllocator.UNSAFE) {
            // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
            // encode. Due to our page size limitation, though, we can convert this into an offset that's
            // relative to the page's base offset; this relative offset will fit in 51 bits.
            offsetInPage -= page.getBaseOffset();
        }
        return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
    }

    Object getPage(long pagePlusOffsetAddress) {
        if (allocator == MemoryAllocator.HEAP) {
            final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
            assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
            final MemoryBlock page = pageTable[pageNumber];
            assert (page != null);
            assert (page.getBaseObject() != null);
            return page.getBaseObject();
        } else {
            return null;
        }
    }

    long getOffsetInPage(long pagePlusOffsetAddress) {
        final long offsetInPage = decodeOffset(pagePlusOffsetAddress);
        if (allocator == MemoryAllocator.HEAP) {
            return offsetInPage;
        } else {
            // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
            // converted the absolute address into a relative address. Here, we invert that operation:
            final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
            assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
            final MemoryBlock page = pageTable[pageNumber];
            assert (page != null);
            return page.getBaseOffset() + offsetInPage;
        }
    }

    private static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
        assert (pageNumber >= 0) : "encodePageNumberAndOffset called with invalid page";
        return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
    }

    private static int decodePageNumber(long pagePlusOffsetAddress) {
        return (int) (pagePlusOffsetAddress >>> OFFSET_BITS);
    }

    private static long decodeOffset(long pagePlusOffsetAddress) {
        return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
    }

    public final long getUsedMemory() {
        return used;
    }
}