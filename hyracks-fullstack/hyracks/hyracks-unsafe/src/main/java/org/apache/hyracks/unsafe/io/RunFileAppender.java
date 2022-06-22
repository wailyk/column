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
package org.apache.hyracks.unsafe.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.unsafe.BytesToBytesMap.Location;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;

public class RunFileAppender {
    private final RunFileWriter runWriter;
    private final ByteBuffer writeBuffer;
    private final List<GeneratedRunFileReader> runs;

    public RunFileAppender(RunFileWriter runWriter, ByteBuffer writeBuffer) {
        this.runWriter = runWriter;
        this.writeBuffer = writeBuffer;
        runs = new ArrayList<>();
    }

    public void open() throws HyracksDataException {
        runWriter.open();
    }

    public void append(Location location) throws HyracksDataException {
        Object baseObject = location.getKeyBase();
        int uaoSize = UnsafeAlignedOffset.getUaoSize();
        //(record length) (key length) (key) (value)
        long offset = location.getKeyOffset() - uaoSize * 2L;
        int recordSize = UnsafeAlignedOffset.getSize(baseObject, offset);
        if (recordSize > writeBuffer.remaining()) {
            write();
        }
        int position = writeBuffer.position();
        int destOffset = position + Platform.BYTE_ARRAY_OFFSET;
        Platform.copyMemory(baseObject, offset, writeBuffer.array(), destOffset, recordSize);
        writeBuffer.position(position + recordSize);
    }

    public List<GeneratedRunFileReader> getRuns() {
        return runs;
    }

    private void write() throws HyracksDataException {
        writeBuffer.flip();
        runWriter.nextFrame(writeBuffer);
        writeBuffer.clear();
    }

    public void close() throws HyracksDataException {
        write();
        runWriter.close();
    }
}
