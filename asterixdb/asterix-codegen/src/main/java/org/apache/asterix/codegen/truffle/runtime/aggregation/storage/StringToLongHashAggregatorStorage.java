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
import static org.apache.asterix.codegen.asterix.map.UnsafeComparators.STRING_COMPARATOR;

import org.apache.asterix.codegen.asterix.map.IUnsafeHashAggregatorFactory;
import org.apache.asterix.codegen.asterix.map.UnsafeAggregators;
import org.apache.asterix.codegen.asterix.map.entry.LongEntry;
import org.apache.asterix.codegen.asterix.map.entry.StringEntry;
import org.apache.asterix.codegen.asterix.map.entry.StringEntryUtil;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.unsafe.BytesToBytesMap.Location;
import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.spark.unsafe.Platform;

public class StringToLongHashAggregatorStorage extends AbstractHashAggregatorStorage {
    private final StringEntry mapKey;
    private final LongEntry mapValue;

    StringToLongHashAggregatorStorage(IUnsafeHashAggregatorFactory aggFactory, String aggType,
            AILResultWriter resultWriter) {
        super(aggFactory, resultWriter, UnsafeAggregators.getLongAggregator(aggType), STRING_COMPARATOR,
                LONG_COMPARATOR);
        mapKey = new StringEntry();
        mapValue = new LongEntry();
    }

    public void add(AILStringRuntime key, long value) {
        mapKey.reset(key.getStringValue());
        mapValue.reset(value);
        aggregate(mapKey, mapValue);
    }

    @Override
    public void appendKey(AILResultWriter resultWriter, Location location) {
        Object baseObject = location.getKeyBase();
        long offset = location.getKeyOffset();
        long alignedLength = location.getKeyLength();
        int encodedLength = StringEntryUtil.decode(baseObject, offset, alignedLength);
        int actualLength = encodedLength + VarLenIntEncoderDecoder.getBytesRequired(encodedLength);
        resultWriter.append(ATypeTag.STRING, baseObject, offset, actualLength);
    }

    @Override
    public void appendKey(AILResultWriter resultWriter, IEntry entry) {
        IValueReference reference = ((StringEntry) entry).getValue();
        int encodedLength = VarLenIntEncoderDecoder.decode(reference.getByteArray(), reference.getStartOffset());
        int actualLength = encodedLength + VarLenIntEncoderDecoder.getBytesRequired(encodedLength);
        resultWriter.append(ATypeTag.STRING, reference.getByteArray(), reference.getStartOffset(), actualLength);
    }

    @Override
    public void appendValue(AILResultWriter resultWriter, IEntry entry) {
        resultWriter.append(((LongEntry) entry).getValue());
    }

    @Override
    public void appendValue(AILResultWriter resultWriter, Location location) {
        resultWriter.append(Platform.getLong(location.getValueBase(), location.getValueOffset()));
    }

    @Override
    public HashTableType getType() {
        return HashTableType.STRING_LONG;
    }
}
