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

import static org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.hyracks.unsafe.entry.IEntryComparator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;

import com.google.common.annotations.VisibleForTesting;

/**
 * Repurposed from Apache Spark
 * <a href="https://github.com/apache/spark/blob/6b5a1f9df28262fa90d28dc15af67e8a37a9efcf/core/src/main/java/org/apache/spark/unsafe/map/BytesToBytesMap.java"></a>
 * <p>
 * An append-only hash map where keys and values are contiguous regions of bytes.
 * <p>
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 * <p>
 * The map can support up to 2^29 keys. If the key cardinality is higher than this, you should
 * probably be using sorting instead of hashing for better cache locality.
 * <p>
 * The key and values under the hood are stored together, in the following format:
 * First uaoSize bytes: len(k) (key length in bytes) + len(v) (value length in bytes) + uaoSize
 * Next uaoSize bytes: len(k)
 * Next len(k) bytes: key data
 * Next len(v) bytes: value data
 * Last 8 bytes: pointer to next pair
 * <p>
 * It means first uaoSize bytes store the entire record (key + value + uaoSize) length. This format
 * is compatible with {@link org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter},
 * so we can pass records from this map directly into the sorter to sort records in place.
 */
public final class BytesToBytesMap extends MemoryConsumer {

    public static final int SEED = 42;

    /**
     * A linked list for tracking all allocated data pages so that we can free all of our memory.
     */
    private final LinkedList<MemoryBlock> dataPages = new LinkedList<>();
    private final UnsafeSortComparator sortComparator;

    /**
     * The data page that will be used to store keys and values for new hashtable entries. When this
     * page becomes full, a new page will be allocated and this pointer will change to point to that
     * new page.
     */
    private MemoryBlock currentPage = null;

    /**
     * Offset into `currentPage` that points to the location where new data can be inserted into
     * the page. This does not incorporate the page's base offset.
     */
    private long pageCursor = 0;

    /**
     * The maximum number of keys that BytesToBytesMap supports. The hash table has to be
     * power-of-2-sized and its backing Java array can contain at most (1 &lt;&lt; 30) elements,
     * since that's the largest power-of-2 that's less than Integer.MAX_VALUE. We need two long array
     * entries per key, giving us a maximum capacity of (1 &lt;&lt; 29).
     */
    public static final int MAX_CAPACITY = (1 << 29);

    // This choice of page table size and page size means that we can address up to 500 gigabytes
    // of memory.

    /**
     * A single array to store the key and value.
     * <p>
     * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
     * while position {@code 2 * i + 1} in the array holds key's full 32-bit hashcode.
     */
    private LongArray longArray;
    // TODO: we're wasting 32 bits of space here; we can probably store fewer bits of the hashcode
    // and exploit word-alignment to use fewer bits to hold the address.  This might let us store
    // only one long per map entry, increasing the chance that this array will fit in cache at the
    // expense of maybe performing more lookups if we have hash collisions.  Say that we stored only
    // 27 bits of the hashcode and 37 bits of the address.  37 bits is enough to address 1 terabyte
    // of RAM given word-alignment.  If we use 13 bits of this for our page table, that gives us a
    // maximum page size of 2^24 * 8 = ~134 megabytes per page. This change will require us to store
    // full base addresses in the page table for off-heap mode so that we can reconstruct the full
    // absolute memory addresses.

    /**
     * Whether or not the longArray can grow. We will not insert more elements if it's false.
     */
    private boolean canGrowArray = true;

    private final double loadFactor;

    /**
     * The size of the data pages that hold key and value data. Map entries cannot span multiple
     * pages, so this limits the maximum entry size.
     */
    private final long pageSizeBytes;

    /**
     * Number of keys defined in the map.
     */
    private int numKeys;

    /**
     * Number of values defined in the map. A key could have multiple values.
     */
    private int numValues;

    /**
     * The map will be expanded once the number of keys exceeds this threshold.
     */
    private int growthThreshold;

    /**
     * Mask for truncating hashcodes so that they do not exceed the long array's size.
     * This is a strength reduction optimization; we're essentially performing a modulus operation,
     * but doing so with a bitmask because this is a power-of-2-sized hash map.
     */
    private int mask;

    /**
     * Return value of {@link BytesToBytesMap#lookup(Object, long, int)}.
     */
    private final Location loc;

    private long numProbes = 0L;

    private long numKeyLookups = 0L;

    private long peakMemoryUsedBytes = 0L;

    private final int initialCapacity;

    private MapIterator destructiveIterator = null;

    public BytesToBytesMap(MemoryAllocator allocator, long budget, int initialCapacity,
            IEntryComparator keyComparator) {
        super(allocator, budget);
        this.loadFactor = 0.8d;
        this.loc = new Location();
        //1MB
        this.pageSizeBytes = PAGE_SIZE;
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("Initial capacity must be greater than 0");
        }
        if (initialCapacity > MAX_CAPACITY) {
            throw new IllegalArgumentException(
                    "Initial capacity " + initialCapacity + " exceeds maximum capacity of " + MAX_CAPACITY);
        }
        this.initialCapacity = initialCapacity;
        allocate(initialCapacity);
        this.sortComparator = new UnsafeSortComparator(keyComparator, this);
    }

    /**
     * Returns the number of keys defined in the map.
     */
    public int numKeys() {
        return numKeys;
    }

    /**
     * Returns the number of values defined in the map. A key could have multiple values.
     */
    public int numValues() {
        return numValues;
    }

    public Location createLocation() {
        return new Location();
    }

    public void remove(Location minLocation) {

    }

    public final class MapIterator implements Iterator<Location> {

        private int numRecords;
        private final Location loc;

        private MemoryBlock currentPage = null;
        private int recordsInPage = 0;
        private Object pageBaseObject;
        private long offsetInPage;

        // If this iterator destructive or not. When it is true, it frees each page as it moves onto
        // next one.
        private boolean destructive = false;

        private MapIterator(int numRecords, Location loc, boolean destructive) {
            this.numRecords = numRecords;
            this.loc = loc;
            this.destructive = destructive;
            if (destructive) {
                destructiveIterator = this;
                // longArray will not be used anymore if destructive is true, release it now.
                if (longArray != null) {
                    freeArray(longArray);
                    longArray = null;
                }
            }
        }

        private void advanceToNextPage() {
            MemoryBlock pageToFree = null;

            try {
                int nextIdx = dataPages.indexOf(currentPage) + 1;
                if (destructive && currentPage != null) {
                    dataPages.remove(currentPage);
                    pageToFree = currentPage;
                    nextIdx--;
                }
                if (dataPages.size() > nextIdx) {
                    currentPage = dataPages.get(nextIdx);
                    pageBaseObject = currentPage.getBaseObject();
                    offsetInPage = currentPage.getBaseOffset();
                    recordsInPage = UnsafeAlignedOffset.getSize(pageBaseObject, offsetInPage);
                    offsetInPage += UnsafeAlignedOffset.getUaoSize();
                } else {
                    currentPage = null;
                    recordsInPage = -1;
                }
            } finally {
                if (pageToFree != null) {
                    freePage(pageToFree);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return numRecords > 0;
        }

        @Override
        public Location next() {
            if (recordsInPage == 0) {
                advanceToNextPage();
            }
            numRecords--;
            if (currentPage != null) {
                int totalLength = UnsafeAlignedOffset.getSize(pageBaseObject, offsetInPage);
                loc.with(currentPage, offsetInPage);
                // [total size] [key size] [key] [value] [pointer to next]
                offsetInPage += UnsafeAlignedOffset.getUaoSize() + totalLength + 8;
                recordsInPage--;
            } else {
                //Simple fix to remove the last page
                dataPages.clear();
            }
            return loc;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public void reset() {
            numRecords = numKeys;
            currentPage = null;
            recordsInPage = 0;
        }
    }

    /**
     * Returns an iterator for iterating over the entries of this map.
     * <p>
     * For efficiency, all calls to `next()` will return the same {@link Location} object.
     * <p>
     * The returned iterator is thread-safe. However if the map is modified while iterating over it,
     * the behavior of the returned iterator is undefined.
     */
    public MapIterator iterator() {
        return new MapIterator(numValues, new Location(), false);
    }

    public SortedIterator sortedIterator() {
        return new SortedIterator();
    }

    /**
     * Returns a destructive iterator for iterating over the entries of this map. It frees each page
     * as it moves onto next one. Notice: it is illegal to call any method on the map after
     * `destructiveIterator()` has been called.
     * <p>
     * For efficiency, all calls to `next()` will return the same {@link Location} object.
     * <p>
     * The returned iterator is thread-safe. However if the map is modified while iterating over it,
     * the behavior of the returned iterator is undefined.
     */
    public MapIterator getDestructiveIterator() {
        updatePeakMemoryUsed();
        return new MapIterator(numValues, new Location(), true);
    }

    /**
     * Iterator for the entries of this map. This is to first iterate over key indices in
     * `longArray` then accessing values in `dataPages`. NOTE: this is different from `MapIterator`
     * in the sense that key index is preserved here
     * (See `UnsafeHashedRelation` for example of usage).
     */
    public final class MapIteratorWithKeyIndex implements Iterator<Location> {

        /**
         * The index in `longArray` where the key is stored.
         */
        private int keyIndex = 0;

        private int numRecords;
        private final Location loc;

        private MapIteratorWithKeyIndex() {
            this.numRecords = numValues;
            this.loc = new Location();
        }

        @Override
        public boolean hasNext() {
            return numRecords > 0;
        }

        @Override
        public Location next() {
            if (!loc.isDefined() || !loc.nextValue()) {
                while (longArray.get(keyIndex * 2) == 0) {
                    keyIndex++;
                }
                loc.with(keyIndex, 0, true);
                keyIndex++;
            }
            numRecords--;
            return loc;
        }
    }

    /**
     * Iterator for the entries of this map. This is to first iterate over key indices in
     * `longArray` then accessing values in `dataPages`. NOTE: this is different from `MapIterator`
     * in the sense that key index is preserved here
     * (See `UnsafeHashedRelation` for example of usage).
     */
    public final class SortedIterator implements Iterator<Location> {

        /**
         * The index in `longArray` where the key is stored.
         */
        private int keyIndex = 0;

        private int numRecords;
        private final Location loc;

        private SortedIterator() {
            this.numRecords = numValues;
            this.loc = new Location();
        }

        @Override
        public boolean hasNext() {
            return numRecords > 0;
        }

        @Override
        public Location next() {
            loc.with(keyIndex, 0, true);
            keyIndex++;
            numRecords--;
            return loc;
        }
    }

    /**
     * Returns an iterator for iterating over the entries of this map,
     * by first iterating over the key index inside hash map's `longArray`.
     * <p>
     * For efficiency, all calls to `next()` will return the same {@link Location} object.
     * <p>
     * The returned iterator is NOT thread-safe. If the map is modified while iterating over it,
     * the behavior of the returned iterator is undefined.
     */
    public MapIteratorWithKeyIndex iteratorWithKeyIndex() {
        return new MapIteratorWithKeyIndex();
    }

    /**
     * The maximum number of allowed keys index.
     * <p>
     * The value of allowed keys index is in the range of [0, maxNumKeysIndex - 1].
     */
    public int maxNumKeysIndex() {
        return (int) (longArray.size() / 2);
    }

    /**
     * Looks up a key, and return a {@link Location} handle that can be used to test existence
     * and read/write values.
     * <p>
     * This function always returns the same {@link Location} instance to avoid object allocation.
     * This function is not thread-safe.
     */
    public Location lookup(IEntry key) {
        safeLookup(key, loc, key.getHash());
        return loc;
    }

    public void sort() {
        Iterator<Location> iter = iterator();
        int i = 0;
        while (iter.hasNext()) {
            Location location = iter.next();
            Object baseObject = location.getKeyBase();
            long offset = location.getKeyOffset();
            int length = location.getKeyLength();

            long prefix = sortComparator.computePrefix(baseObject, offset, length);

            // Get encoded memory address
            // baseObject + baseOffset point to the beginning of the key data in the map, but that
            // the KV-pair's length data is stored at 2 * uaoSize bytes immediately before that address
            MemoryBlock page = location.getMemoryPage();
            long address = encodePageNumberAndOffset(page, offset - 2L * UnsafeAlignedOffset.getUaoSize());
            longArray.set(i, address);
            i++;
            longArray.set(i, prefix);
            i++;
        }
        expandPointerArrayIfNeeded();
        MemoryBlock unused = new MemoryBlock(longArray.getBaseObject(), longArray.getBaseOffset() + i * 8L,
                (longArray.size() - i) * 8L);
        LongArray buffer = new LongArray(unused);
        TimSort sorter = new TimSort(new UnsafeSortDataFormat(buffer));
        sorter.sort(longArray, 0, numKeys, sortComparator);
    }

    public void expandPointerArrayIfNeeded() {
        long neededSize = (long) Math.ceil(numValues * 2 * 1.5);
        if (neededSize >= longArray.size()) {
            LongArray oldArray = longArray;
            long newSize = (long) Math.ceil(numValues * 1.5) * 2;
            //We will tolerate a slight increase here, and we will ignore the budget constraint
            longArray = allocateArray(newSize, newSize);
            Platform.copyMemory(oldArray.getBaseObject(), oldArray.getBaseOffset(), longArray.getBaseObject(),
                    longArray.getBaseOffset(), oldArray.size() * 8);
            freeArray(oldArray);
        }
    }

    /**
     * Looks up a key, and saves the result in provided `loc`.
     * <p>
     * This is a thread-safe version of `lookup`, could be used by multiple threads.
     */
    private void safeLookup(IEntry key, Location loc, int hash) {
        assert (longArray != null);

        numKeyLookups++;

        int pos = hash & mask;
        int step = 1;
        while (true) {
            numProbes++;
            if (longArray.get(pos * 2) == 0) {
                // This is a new key.
                loc.with(pos, hash, false);
                return;
            } else {
                long stored = longArray.get(pos * 2 + 1);
                if ((int) (stored) == hash) {
                    // Full hash code matches.  Let's compare the keys for equality.
                    loc.with(pos, hash, true);
                    if (key.isEqual(loc)) {
                        return;
                    }
                }
            }
            pos = (pos + step) & mask;
            step++;
        }
    }

    /**
     * Handle returned by {@link BytesToBytesMap#lookup(Object, long, int)} function.
     */
    public final class Location {
        /** An index into the hash map's Long array */
        private int pos;
        /** True if this location points to a position where a key is defined, false otherwise */
        private boolean isDefined;
        /**
         * The hashcode of the most recent key passed to
         * {@link BytesToBytesMap#lookup(Object, long, int, int)}. Caching this hashcode here allows us
         * to avoid re-hashing the key when storing a value for that key.
         */
        private int keyHashcode;
        private Object baseObject = null; // the base object for key and value
        private long keyOffset;
        private int keyLength;
        private long valueOffset;
        private int valueLength;

        /**
         * Memory page containing the record. Only set if created by {@link BytesToBytesMap#iterator()}.
         */
        private MemoryBlock memoryPage;

        private void updateAddressesAndSizes(long fullKeyAddress) {
            updateAddressesAndSizes(getPage(fullKeyAddress), getOffsetInPage(fullKeyAddress));
        }

        private void updateAddressesAndSizes(final Object base, long offset) {
            baseObject = base;
            final int totalLength = UnsafeAlignedOffset.getSize(base, offset);
            int uaoSize = UnsafeAlignedOffset.getUaoSize();
            offset += uaoSize;
            keyLength = UnsafeAlignedOffset.getSize(base, offset);
            offset += uaoSize;
            keyOffset = offset;
            valueOffset = offset + keyLength;
            valueLength = totalLength - keyLength - uaoSize;
        }

        private Location with(int pos, int keyHashcode, boolean isDefined) {
            assert (longArray != null);
            this.pos = pos;
            this.isDefined = isDefined;
            this.keyHashcode = keyHashcode;
            if (isDefined) {
                final long fullKeyAddress = longArray.get(pos * 2);
                updateAddressesAndSizes(fullKeyAddress);
            }
            return this;
        }

        private Location with(MemoryBlock page, long offsetInPage) {
            this.isDefined = true;
            this.memoryPage = page;
            updateAddressesAndSizes(page.getBaseObject(), offsetInPage);
            return this;
        }

        /**
         * This is only used for spilling
         */
        private Location with(Object base, long offset, int length) {
            this.isDefined = true;
            this.memoryPage = null;
            baseObject = base;
            int uaoSize = UnsafeAlignedOffset.getUaoSize();
            keyOffset = offset + uaoSize;
            keyLength = UnsafeAlignedOffset.getSize(base, offset);
            valueOffset = offset + uaoSize + keyLength;
            valueLength = length - uaoSize - keyLength;
            return this;
        }

        public void with(Location other) {
            isDefined = true;
            baseObject = other.baseObject;

            keyOffset = other.keyOffset;
            keyLength = other.keyLength;

            valueOffset = other.valueOffset;
            valueLength = other.valueLength;
        }

        public boolean sameLocation(Location other) {
            return baseObject == other.baseObject && keyOffset == other.keyOffset;
        }

        /**
         * Find the next pair that has the same key as current one.
         */
        public boolean nextValue() {
            assert isDefined;
            long nextAddr = Platform.getLong(baseObject, valueOffset + valueLength);
            if (nextAddr == 0) {
                return false;
            } else {
                updateAddressesAndSizes(nextAddr);
                return true;
            }
        }

        /**
         * Returns the memory page that contains the current record.
         * This is only valid if this is returned by {@link BytesToBytesMap#iterator()}.
         */
        public MemoryBlock getMemoryPage() {
            return this.memoryPage;
        }

        /**
         * Returns true if the key is defined at this position, and false otherwise.
         */
        public boolean isDefined() {
            return isDefined;
        }

        /**
         * Returns index for key.
         */
        public int getKeyIndex() {
            assert (isDefined);
            return pos;
        }

        /**
         * Returns the base object for key.
         */
        public Object getKeyBase() {
            assert (isDefined);
            return baseObject;
        }

        /**
         * Returns the offset for key.
         */
        public long getKeyOffset() {
            assert (isDefined);
            return keyOffset;
        }

        /**
         * Returns the base object for value.
         */
        public Object getValueBase() {
            assert (isDefined);
            return baseObject;
        }

        /**
         * Returns the offset for value.
         */
        public long getValueOffset() {
            assert (isDefined);
            return valueOffset;
        }

        /**
         * Returns the length of the key defined at this position.
         * Unspecified behavior if the key is not defined.
         */
        public int getKeyLength() {
            assert (isDefined);
            return keyLength;
        }

        /**
         * Returns the length of the value defined at this position.
         * Unspecified behavior if the key is not defined.
         */
        public int getValueLength() {
            assert (isDefined);
            return valueLength;
        }

        /**
         * Append a new value for the key. This method could be called multiple times for a given key.
         * The return value indicates whether the put succeeded or whether it failed because additional
         * memory could not be acquired.
         * <p>
         * It is only valid to call this method immediately after calling `lookup()` using the same key.
         * </p>
         * <p>
         * The key and value must be word-aligned (that is, their sizes must be a multiple of 8).
         * </p>
         * <p>
         * After calling this method, calls to `get[Key|Value]Address()` and `get[Key|Value]Length`
         * will return information on the data stored by this `append` call.
         * </p>
         * <p>
         * As an example usage, here's the proper way to store a new key:
         * </p>
         * <pre>
         *   Location loc = map.lookup(keyBase, keyOffset, keyLength);
         *   if (!loc.isDefined()) {
         *     if (!loc.append(keyBase, keyOffset, keyLength, ...)) {
         *       // handle failure to grow map (by spilling, for example)
         *     }
         *   }
         * </pre>
         * <p>
         * Unspecified behavior if the key is not defined.
         * </p>
         *
         * @return true if the put() was successful and false if the put() failed because memory could
         * not be acquired.
         */
        public boolean append(IEntry key, IEntry value) {
            int klen = key.getLength();
            int vlen = value.getLength();
            assert (klen % 8 == 0);
            assert (vlen % 8 == 0);
            assert (longArray != null);

            // We should not increase number of keys to be MAX_CAPACITY. The usage pattern of this map is
            // lookup + append. If we append key until the number of keys to be MAX_CAPACITY, next time
            // the call of lookup will hang forever because it cannot find an empty slot.
            if (numKeys == MAX_CAPACITY - 1
                    // The map could be reused from last spill (because of no enough memory to grow),
                    // then we don't try to grow again if hit the `growthThreshold`.
                    || !canGrowArray && numKeys >= growthThreshold) {
                return false;
            }

            // Here, we'll copy the data into our data pages. Because we only store a relative offset from
            // the key address instead of storing the absolute address of the value, the key and value
            // must be stored in the same memory page.
            // (total length) (key length) (key) (value) (8 byte pointer to next value)
            int uaoSize = UnsafeAlignedOffset.getUaoSize();
            final long recordLength = (2L * uaoSize) + klen + vlen + 8;
            if (currentPage == null || currentPage.size() - pageCursor < recordLength) {
                if (!acquireNewPage(recordLength + uaoSize)) {
                    return false;
                }
            }

            // --- Append the key and value data to the current data page --------------------------------
            final Object base = currentPage.getBaseObject();
            long offset = currentPage.getBaseOffset() + pageCursor;
            final long recordOffset = offset;
            UnsafeAlignedOffset.putSize(base, offset, klen + vlen + uaoSize);
            UnsafeAlignedOffset.putSize(base, offset + uaoSize, klen);
            offset += (2L * uaoSize);
            key.set(base, offset, klen);
            offset += klen;
            value.set(base, offset, vlen);
            offset += vlen;
            // put this value at the beginning of the list
            Platform.putLong(base, offset, isDefined ? longArray.get(pos * 2) : 0);

            // --- Update bookkeeping data structures ----------------------------------------------------
            offset = currentPage.getBaseOffset();
            UnsafeAlignedOffset.putSize(base, offset, UnsafeAlignedOffset.getSize(base, offset) + 1);
            pageCursor += recordLength;
            final long storedKeyAddress = encodePageNumberAndOffset(currentPage, recordOffset);
            longArray.set(pos * 2, storedKeyAddress);
            updateAddressesAndSizes(storedKeyAddress);
            numValues++;
            if (!isDefined) {
                numKeys++;
                longArray.set(pos * 2 + 1, keyHashcode);
                isDefined = true;

                // If the map has reached its growth threshold, try to grow it.
                if (numKeys >= growthThreshold) {
                    // We use two array entries per key, so the array size is twice the capacity.
                    // We should compare the current capacity of the array, instead of its size.
                    int arraySize = (int) longArray.size();
                    if (arraySize / 2 < MAX_CAPACITY && canAllocateArray(nextCapacity(arraySize), arraySize)) {
                        growAndRehash();
                    } else {
                        // The map is already at MAX_CAPACITY and cannot grow. Instead, we prevent it from
                        // accepting any more new elements to make sure we don't exceed the load factor. If we
                        // need to spill later, this allows UnsafeKVExternalSorter to reuse the array for
                        // sorting.
                        canGrowArray = false;
                    }
                }
            }
            return true;
        }

        public void unset() {
            isDefined = false;
        }
    }

    /**
     * Acquire a new page from the memory manager.
     *
     * @return whether there is enough space to allocate the new page.
     */
    private boolean acquireNewPage(long required) {
        currentPage = allocatePage(required);
        if (currentPage == null) {
            return false;
        }
        dataPages.add(currentPage);
        UnsafeAlignedOffset.putSize(currentPage.getBaseObject(), currentPage.getBaseOffset(), 0);
        pageCursor = UnsafeAlignedOffset.getUaoSize();
        return true;
    }

    /**
     * Allocate new data structures for this map. When calling this outside of the constructor,
     * make sure to keep references to the old data structures so that you can free them.
     *
     * @param capacity the new map capacity
     */
    private boolean allocate(int capacity) {
        assert (capacity >= 0);
        capacity = Math.max((int) Math.min(MAX_CAPACITY, ByteArrayMethods.nextPowerOf2(capacity)), 64);
        assert (capacity <= MAX_CAPACITY);
        LongArray newArray = allocateArray(capacity * 2L, longArray == null ? 0 : longArray.size());
        if (newArray == null) {
            return false;
        }
        longArray = newArray;
        longArray.zeroOut();

        this.growthThreshold = (int) (capacity * loadFactor);
        this.mask = capacity - 1;
        return true;
    }

    /**
     * Free all allocated memory associated with this map, including the storage for keys and values
     * as well as the hash map array itself.
     * <p>
     * This method is idempotent and can be called multiple times.
     */
    public void free() {
        updatePeakMemoryUsed();
        if (longArray != null) {
            freeArray(longArray);
            longArray = null;
        }
        Iterator<MemoryBlock> dataPagesIterator = dataPages.iterator();
        while (dataPagesIterator.hasNext()) {
            MemoryBlock dataPage = dataPagesIterator.next();
            dataPagesIterator.remove();
            freePage(dataPage);
        }
        assert (dataPages.isEmpty());
    }

    public long getPageSizeBytes() {
        return pageSizeBytes;
    }

    /**
     * Returns the total amount of memory, in bytes, consumed by this map's managed structures.
     */
    public long getTotalMemoryConsumption() {
        long totalDataPagesSize = 0L;
        for (MemoryBlock dataPage : dataPages) {
            totalDataPagesSize += dataPage.size();
        }
        return totalDataPagesSize + ((longArray != null) ? longArray.memoryBlock().size() : 0L);
    }

    private void updatePeakMemoryUsed() {
        long mem = getTotalMemoryConsumption();
        if (mem > peakMemoryUsedBytes) {
            peakMemoryUsedBytes = mem;
        }
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    public long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
    }

    /**
     * Returns the average number of probes per key lookup.
     */
    public double getAvgHashProbeBucketListIterations() {
        return (1.0 * numProbes) / numKeyLookups;
    }

    public int getNumDataPages() {
        return dataPages.size();
    }

    /**
     * Returns the underline long[] of longArray.
     */
    public LongArray getArray() {
        assert (longArray != null);
        return longArray;
    }

    /**
     * Reset this map to initialized state.
     */
    public void reset() {
        updatePeakMemoryUsed();
        numKeys = 0;
        numValues = 0;
        freeArray(longArray);
        longArray = null;
        while (!dataPages.isEmpty()) {
            MemoryBlock dataPage = dataPages.removeLast();
            freePage(dataPage);
        }
        allocate(initialCapacity);
        canGrowArray = true;
        currentPage = null;
        pageCursor = 0;
    }

    /**
     * Grows the size of the hash table and re-hash everything.
     */
    @VisibleForTesting
    void growAndRehash() {
        assert (longArray != null);

        // Store references to the old data structures to be used when we re-hash
        final LongArray oldLongArray = longArray;
        final int oldCapacity = (int) oldLongArray.size() / 2;

        // Allocate the new data structures
        allocate(Math.min(nextCapacity(oldCapacity), MAX_CAPACITY));

        // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
        for (int i = 0; i < oldLongArray.size(); i += 2) {
            final long keyPointer = oldLongArray.get(i);
            if (keyPointer == 0) {
                continue;
            }
            final int hashcode = (int) oldLongArray.get(i + 1);
            int newPos = hashcode & mask;
            int step = 1;
            while (longArray.get(newPos * 2) != 0) {
                newPos = (newPos + step) & mask;
                step++;
            }
            longArray.set(newPos * 2, keyPointer);
            longArray.set(newPos * 2 + 1, hashcode);
        }
        freeArray(oldLongArray);
    }

    private static int nextCapacity(int currentCapacity) {
        assert (currentCapacity > 0);
        int doubleCapacity = currentCapacity * 2;
        // Guard against overflow
        return (doubleCapacity > 0 && doubleCapacity <= MAX_ROUNDED_ARRAY_LENGTH) ? doubleCapacity
                : MAX_ROUNDED_ARRAY_LENGTH;
    }
}