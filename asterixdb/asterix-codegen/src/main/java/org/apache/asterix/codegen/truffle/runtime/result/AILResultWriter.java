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
package org.apache.asterix.codegen.truffle.runtime.result;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.AILRuntimeException;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.spark.unsafe.Platform;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

@ExportLibrary(InteropLibrary.class)
public class AILResultWriter implements TruffleObject {
    private final IHyracksTaskContext context;
    private final DataOutput dos;
    private final ArrayTupleBuilder tb;
    private final IFrameWriter writer;
    private final FrameTupleAppender appender;

    public AILResultWriter(IHyracksTaskContext context, DataOutput dos, ArrayTupleBuilder tb, IFrameWriter writer,
            FrameTupleAppender appender) {
        this.context = context;
        this.dos = dos;
        this.tb = tb;
        this.writer = writer;
        this.appender = appender;
    }

    public void appendMissing() {
        try {
            dos.writeByte(ATypeTag.MISSING.serialize());
            tb.addFieldEndOffset();
        } catch (IOException e) {
            throw new AILRuntimeException();
        }
    }

    public void appendNull() {
        try {
            dos.writeByte(ATypeTag.NULL.serialize());
            tb.addFieldEndOffset();
        } catch (IOException e) {
            throw new AILRuntimeException();
        }
    }

    public void append(long value) {
        try {
            dos.writeByte(ATypeTag.BIGINT.serialize());
            dos.writeLong(value);
            tb.addFieldEndOffset();
        } catch (IOException e) {
            throw new AILRuntimeException();
        }
    }

    public void append(double value) {
        try {
            dos.writeByte(ATypeTag.DOUBLE.serialize());
            dos.writeDouble(value);
            tb.addFieldEndOffset();
        } catch (IOException e) {
            throw new AILRuntimeException();
        }
    }

    public void append(AILStringRuntime stringRuntime) {
        append(ATypeTag.STRING, stringRuntime.getStringValue());
    }

    public void append(ATypeTag typeTag, IValueReference value) {
        try {
            dos.writeByte(typeTag.serialize());
            dos.write(value.getByteArray(), value.getStartOffset(), value.getLength());
            tb.addFieldEndOffset();
        } catch (IOException e) {
            throw new AILRuntimeException();
        }
    }

    public void append(ATypeTag typeTag, byte[] bytes, int offset, int length) {
        try {
            dos.writeByte(typeTag.serialize());
            dos.write(bytes, offset, length);
            tb.addFieldEndOffset();
        } catch (IOException e) {
            throw new AILRuntimeException();
        }
    }

    public void append(ATypeTag typeTag, Object baseObject, long offset, int length) {
        GrowableArray fieldArray = tb.getFieldData();
        int typeTagOffset = fieldArray.getLength();
        int writeOffset = typeTagOffset + 1;
        fieldArray.setSize(writeOffset + length);

        byte[] bytes = fieldArray.getByteArray();
        int unsafeOffset = writeOffset + Platform.BYTE_ARRAY_OFFSET;
        bytes[typeTagOffset] = typeTag.serialize();
        Platform.copyMemory(baseObject, offset, bytes, unsafeOffset, length);
        tb.addFieldEndOffset();
    }

    public void flush() {
        try {
            if (tb.getSize() > 0) {
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
                tb.reset();
            }
        } catch (HyracksDataException e) {
            throw new AILRuntimeException();
        }
    }

    @ExportMessage
    boolean hasLanguage() {
        return true;
    }

    @ExportMessage
    Class<? extends TruffleLanguage<?>> getLanguage() {
        return AILLanguage.class;
    }

    @ExportMessage
    Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects) {
        return "appender";
    }

}
