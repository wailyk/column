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

package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.asterix.om.base.ABinary;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;

public class ABinarySerializerDeserializer implements ISerializerDeserializer<ABinary> {

    private static final long serialVersionUID = 1L;
    public static final ABinarySerializerDeserializer INSTANCE = new ABinarySerializerDeserializer();

    private ABinarySerializerDeserializer() {
    }

    @Override
    public ABinary deserialize(DataInput in) throws HyracksDataException {
        return new ABinary(ByteArraySerializerDeserializer.read(in));
    }

    @Override
    public void serialize(ABinary binary, DataOutput out) throws HyracksDataException {
        ByteArraySerializerDeserializer.serialize(binary.getBytes(), binary.getStart(), binary.getLength(), out);
    }

    public static int getContentLength(byte[] bytes, int offset) {
        return ByteArrayPointable.getContentLength(bytes, offset);
    }

    public static int getMetaLength(int contentLength) {
        return ByteArrayPointable.getNumberBytesToStoreMeta(contentLength);
    }
}
