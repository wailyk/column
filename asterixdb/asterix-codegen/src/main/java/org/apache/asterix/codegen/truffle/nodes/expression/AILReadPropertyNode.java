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
import org.apache.asterix.codegen.truffle.runtime.array.AILArray;
import org.apache.asterix.codegen.truffle.runtime.array.storage.ArrayStorageNodes.GetNode;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * The node for reading a property of an object. When executed, this node:
 * <ol>
 * <li>evaluates the object expression on the left hand side of the object access operator</li>
 * <li>evaluated the property name</li>
 * <li>reads the named property</li>
 * reader0.getValue()
 * </ol>
 */
@NodeInfo(shortName = ".")
@NodeChild("receiverNode")
@NodeChild("nameNode")
public abstract class AILReadPropertyNode extends AILExpressionNode {

    static final int LIBRARY_LIMIT = 6;

    @CompilationFinal
    Object member;

    @Specialization
    protected Object readMember(AILArray array, long index, @Cached GetNode getNode) {
        return getNode.execute(array.getStorage(), index);
    }

    @Specialization
    protected Object readMember(AILArray array, String name) {
        Object cachedMember = member;
        if (cachedMember == null) {
            CompilerDirectives.transferToInterpreterAndInvalidate();
            cachedMember = member = array.readMember(name);
        }
        return cachedMember;
    }

    @Specialization(guards = { "objects.hasMembers(receiver)" }, limit = "LIBRARY_LIMIT")
    protected Object readObjectFirstTime(Object receiver, Object name,
            @CachedLibrary("receiver") InteropLibrary objects, @Cached SLToMemberNode asMember) {
        Object cachedMember = member;
        if (cachedMember == null) {
            CompilerDirectives.transferToInterpreterAndInvalidate();
            try {
                cachedMember = member = objects.readMember(receiver, asMember.execute(name));
            } catch (UnsupportedMessageException | UnknownIdentifierException e) {
                // read was not successful. In SL we only have basic support for errors.
                throw AILUndefinedNameException.undefinedProperty(this, name);
            }
        }
        return cachedMember;
    }
}
