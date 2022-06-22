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
package org.apache.asterix.codegen.common;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class TruffleTestUtil {
    private static final AStringSerializerDeserializer STRING_SER_DER =
            new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
    private static final AMutableString MUTABLE_STRING = new AMutableString("");

    private TruffleTestUtil() {
    }

    public static AILStringRuntime createTruffleString(String value) throws HyracksDataException {
        return setValue(value, new ArrayBackedValueStorage(), new AILStringRuntime());
    }

    public static AILStringRuntime setValue(String value, ArrayBackedValueStorage storage, AILStringRuntime ailString)
            throws HyracksDataException {
        storage.reset();
        MUTABLE_STRING.setValue(value);
        STRING_SER_DER.serialize(MUTABLE_STRING, storage.getDataOutput());
        ailString.reset(storage);
        return ailString;
    }

    public static String createJavaString(AILStringRuntime value) throws HyracksDataException {
        IValueReference valueRef = value.getStringValue();
        byte[] b = valueRef.getByteArray();
        int offset = valueRef.getStartOffset();
        int length = valueRef.getLength();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(b, offset, length));
        return STRING_SER_DER.deserialize(in).getStringValue();
    }
}
