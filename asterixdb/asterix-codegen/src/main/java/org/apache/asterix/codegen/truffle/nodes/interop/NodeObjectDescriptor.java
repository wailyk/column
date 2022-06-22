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
package org.apache.asterix.codegen.truffle.nodes.interop;

import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.instrumentation.StandardTags;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.profiles.BranchProfile;
import com.oracle.truffle.api.source.SourceSection;

/**
 * A container class used to store per-node attributes used by the instrumentation framework.
 */
public abstract class NodeObjectDescriptor implements TruffleObject {

    private final String name;

    private NodeObjectDescriptor(String name) {
        assert name != null;
        this.name = name;
    }

    public static NodeObjectDescriptor readVariable(String name) {
        return new ReadDescriptor(name);
    }

    public static NodeObjectDescriptor writeVariable(String name, SourceSection sourceSection) {
        return new WriteDescriptor(name, sourceSection);
    }

    Object readMember(String member, @Cached BranchProfile error) throws UnknownIdentifierException {
        if (isMemberReadable(member)) {
            return name;
        } else {
            error.enter();
            throw UnknownIdentifierException.create(member);
        }
    }

    abstract boolean isMemberReadable(String member);

    @ExportLibrary(InteropLibrary.class)
    static final class ReadDescriptor extends NodeObjectDescriptor {

        private static final TruffleObject KEYS_READ = new NodeObjectDescriptorKeys(StandardTags.ReadVariableTag.NAME);

        ReadDescriptor(String name) {
            super(name);
        }

        @ExportMessage
        @SuppressWarnings("static-method")
        boolean hasMembers() {
            return true;
        }

        @Override
        @ExportMessage
        boolean isMemberReadable(String member) {
            return StandardTags.ReadVariableTag.NAME.equals(member);
        }

        @ExportMessage
        @SuppressWarnings("static-method")
        Object getMembers(@SuppressWarnings("unused") boolean includeInternal) {
            return KEYS_READ;
        }

        @Override
        @ExportMessage
        Object readMember(String member, @Cached BranchProfile error) throws UnknownIdentifierException {
            return super.readMember(member, error);
        }

    }

    @ExportLibrary(InteropLibrary.class)
    static final class WriteDescriptor extends NodeObjectDescriptor {

        private static final TruffleObject KEYS_WRITE =
                new NodeObjectDescriptorKeys(StandardTags.WriteVariableTag.NAME);

        private final Object nameSymbol;

        WriteDescriptor(String name, SourceSection sourceSection) {
            super(name);
            this.nameSymbol = new NameSymbol(name, sourceSection);
        }

        @ExportMessage
        @SuppressWarnings("static-method")
        boolean hasMembers() {
            return true;
        }

        @Override
        @ExportMessage
        boolean isMemberReadable(String member) {
            return StandardTags.WriteVariableTag.NAME.equals(member);
        }

        @ExportMessage
        @SuppressWarnings("static-method")
        Object getMembers(@SuppressWarnings("unused") boolean includeInternal) {
            return KEYS_WRITE;
        }

        @Override
        @ExportMessage
        Object readMember(String member, @Cached BranchProfile error) throws UnknownIdentifierException {
            super.readMember(member, error); // To verify readability
            return nameSymbol;
        }
    }

    @ExportLibrary(InteropLibrary.class)
    static final class NameSymbol implements TruffleObject {

        private final String name;
        private final SourceSection sourceSection;

        NameSymbol(String name, SourceSection sourceSection) {
            this.name = name;
            this.sourceSection = sourceSection;
        }

        @ExportMessage
        @SuppressWarnings("static-method")
        boolean isString() {
            return true;
        }

        @ExportMessage
        String asString() {
            return name;
        }

        @ExportMessage
        boolean hasSourceLocation() {
            return sourceSection != null;
        }

        @ExportMessage
        SourceSection getSourceLocation() throws UnsupportedMessageException {
            if (sourceSection != null) {
                return sourceSection;
            } else {
                throw UnsupportedMessageException.create();
            }
        }
    }
}
