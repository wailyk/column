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
package org.apache.asterix.codegen.truffle.nodes.expression;

import org.apache.asterix.codegen.truffle.nodes.AILExpressionNode;
import org.apache.asterix.codegen.truffle.nodes.util.SLToMemberNode;
import org.apache.asterix.codegen.truffle.runtime.AILUndefinedNameException;

import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.InvalidArrayIndexException;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.interop.UnsupportedTypeException;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * The node for writing a property of an object. When executed, this node:
 * <ol>
 * <li>evaluates the object expression on the left hand side of the object access operator</li>
 * <li>evaluates the property name</li>
 * <li>evaluates the value expression on the right hand side of the assignment operator</li>
 * <li>writes the named property</li>
 * <li>returns the written value</li>
 * </ol>
 */
@NodeInfo(shortName = ".=")
@NodeChild("receiverNode")
@NodeChild("nameNode")
@NodeChild("valueNode")
public abstract class AILWritePropertyNode extends AILExpressionNode {

    static final int LIBRARY_LIMIT = 3;

    @Specialization(guards = "arrays.hasArrayElements(receiver)", limit = "LIBRARY_LIMIT")
    protected Object writeArray(Object receiver, Object index, Object value,
            @CachedLibrary("receiver") InteropLibrary arrays, @CachedLibrary("index") InteropLibrary numbers) {
        try {
            arrays.writeArrayElement(receiver, numbers.asLong(index), value);
        } catch (UnsupportedMessageException | UnsupportedTypeException | InvalidArrayIndexException e) {
            // read was not successful. In SL we only have basic support for errors.
            throw AILUndefinedNameException.undefinedProperty(this, index);
        }
        return value;
    }

    @Specialization(limit = "LIBRARY_LIMIT")
    protected Object writeObject(Object receiver, Object name, Object value,
            @CachedLibrary("receiver") InteropLibrary objectLibrary, @Cached SLToMemberNode asMember) {
        try {
            objectLibrary.writeMember(receiver, asMember.execute(name), value);
        } catch (UnsupportedMessageException | UnknownIdentifierException | UnsupportedTypeException e) {
            // write was not successful. In SL we only have basic support for errors.
            throw AILUndefinedNameException.undefinedProperty(this, name);
        }
        return value;
    }

}
