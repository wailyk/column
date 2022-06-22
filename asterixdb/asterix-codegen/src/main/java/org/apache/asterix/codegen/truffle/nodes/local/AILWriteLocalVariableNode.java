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
package org.apache.asterix.codegen.truffle.nodes.local;

import org.apache.asterix.codegen.truffle.nodes.AILExpressionNode;
import org.apache.asterix.codegen.truffle.nodes.interop.NodeObjectDescriptor;
import org.apache.asterix.codegen.truffle.runtime.AILNullRuntime;

import com.oracle.truffle.api.dsl.Fallback;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeField;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.instrumentation.StandardTags.WriteVariableTag;
import com.oracle.truffle.api.instrumentation.Tag;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.source.SourceSection;

/**
 * Node to write a local variable to a function's {@link VirtualFrame frame}. The Truffle frame API
 * allows to store primitive values of all Java primitive types, and Object values.
 */
@NodeChild("valueNode")
@NodeField(name = "slot", type = FrameSlot.class)
@NodeField(name = "nameNode", type = AILExpressionNode.class)
@NodeField(name = "declaration", type = boolean.class)
public abstract class AILWriteLocalVariableNode extends AILExpressionNode {

    /**
     * Returns the descriptor of the accessed local variable. The implementation of this method is
     * created by the Truffle DSL based on the {@link NodeField} annotation on the class.
     */
    protected abstract FrameSlot getSlot();

    /**
     * Returns the child node <code>nameNode</code>. The implementation of this method is created by
     * the Truffle DSL based on the {@link NodeChild} annotation on the class.
     */
    protected abstract AILExpressionNode getNameNode();

    public abstract boolean isDeclaration();

    @Specialization(guards = "isIllegal(frame)")
    protected Object writeNull(VirtualFrame frame, AILNullRuntime value) {
        //Do not write anything if the initial value is null
        return value;
    }

    /**
     * Specialized method to write a primitive {@code long} value. This is only possible if the
     * local variable also has currently the type {@code long} or was never written before,
     * therefore a Truffle DSL {@link #isLongOrIllegal(VirtualFrame) custom guard} is specified.
     */
    @Specialization(guards = "isLongOrIllegal(frame)")
    protected long writeLong(VirtualFrame frame, long value) {
        /* Initialize type on first write of the local variable. No-op if kind is already Long. */
        frame.getFrameDescriptor().setFrameSlotKind(getSlot(), FrameSlotKind.Long);

        frame.setLong(getSlot(), value);
        return value;
    }

    @Specialization(guards = "isBooleanOrIllegal(frame)")
    protected boolean writeBoolean(VirtualFrame frame, boolean value) {
        /* Initialize type on first write of the local variable. No-op if kind is already Long. */
        frame.getFrameDescriptor().setFrameSlotKind(getSlot(), FrameSlotKind.Boolean);

        frame.setBoolean(getSlot(), value);
        return value;
    }

    /**
     * Generic write method that works for all possible types.
     * <p>
     * Why is this method annotated with {@link Specialization} and not {@link Fallback}? For a
     * {@link Fallback} method, the Truffle DSL generated code would try all other specializations
     * first before calling this method. We know that all these specializations would fail their
     * guards, so there is no point in calling them. Since this method takes a value of type
     * {@link Object}, it is guaranteed to never fail, i.e., once we are in this specialization the
     * node will never be re-specialized.
     */
    @Specialization(replaces = { "writeLong", "writeBoolean" })
    protected Object write(VirtualFrame frame, Object value) {
        /*
         * Regardless of the type before, the new and final type of the local variable is Object.
         * Changing the slot kind also discards compiled code, because the variable type is
         * important when the compiler optimizes a method.
         *
         * No-op if kind is already Object.
         */
        frame.getFrameDescriptor().setFrameSlotKind(getSlot(), FrameSlotKind.Object);

        frame.setObject(getSlot(), value);
        return value;
    }

    public abstract void executeWrite(VirtualFrame frame, Object value);

    protected boolean isIllegal(VirtualFrame frame) {
        final FrameSlotKind kind = frame.getFrameDescriptor().getFrameSlotKind(getSlot());
        return kind == FrameSlotKind.Illegal;
    }

    /**
     * Guard function that the local variable has the type {@code long}.
     *
     * @param frame The parameter seems unnecessary, but it is required: Without the parameter, the
     *              Truffle DSL would not check the guard on every execution of the specialization.
     *              Guards without parameters are assumed to be pure, but our guard depends on the
     *              slot kind which can change.
     */
    protected boolean isLongOrIllegal(VirtualFrame frame) {
        final FrameSlotKind kind = frame.getFrameDescriptor().getFrameSlotKind(getSlot());
        return kind == FrameSlotKind.Long || kind == FrameSlotKind.Illegal;
    }

    protected boolean isBooleanOrIllegal(VirtualFrame frame) {
        final FrameSlotKind kind = frame.getFrameDescriptor().getFrameSlotKind(getSlot());
        return kind == FrameSlotKind.Boolean || kind == FrameSlotKind.Illegal;
    }

    @Override
    public boolean hasTag(Class<? extends Tag> tag) {
        return tag == WriteVariableTag.class || super.hasTag(tag);
    }

    @Override
    public Object getNodeObject() {
        AILExpressionNode nameNode = getNameNode();
        SourceSection nameSourceSection;
        if (nameNode.getSourceCharIndex() == -1) {
            nameSourceSection = null;
        } else {
            SourceSection rootSourceSection = getRootNode().getSourceSection();
            if (rootSourceSection == null) {
                nameSourceSection = null;
            } else {
                Source source = rootSourceSection.getSource();
                nameSourceSection = source.createSection(nameNode.getSourceCharIndex(), nameNode.getSourceLength());
            }
        }
        return NodeObjectDescriptor.writeVariable(getSlot().getIdentifier().toString(), nameSourceSection);
    }
}
