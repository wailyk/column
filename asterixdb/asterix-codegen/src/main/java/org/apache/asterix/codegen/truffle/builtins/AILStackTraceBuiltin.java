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
package org.apache.asterix.codegen.truffle.builtins;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.Frame;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameInstance;
import com.oracle.truffle.api.frame.FrameInstance.FrameAccess;
import com.oracle.truffle.api.frame.FrameInstanceVisitor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.RootNode;

/**
 * Returns a string representation of the current stack. This includes the {@link CallTarget}s and
 * the contents of the {@link Frame}.
 */
@NodeInfo(shortName = "stacktrace")
public abstract class AILStackTraceBuiltin extends AILBuiltinNode {

    @Specialization
    public String trace() {
        return createStackTrace();
    }

    @TruffleBoundary
    private static String createStackTrace() {
        final StringBuilder str = new StringBuilder();

        Truffle.getRuntime().iterateFrames(new FrameInstanceVisitor<Integer>() {
            private int skip = 1; // skip stack trace builtin

            @Override
            public Integer visitFrame(FrameInstance frameInstance) {
                if (skip > 0) {
                    skip--;
                    return null;
                }
                CallTarget callTarget = frameInstance.getCallTarget();
                Frame frame = frameInstance.getFrame(FrameAccess.READ_ONLY);
                RootNode rn = ((RootCallTarget) callTarget).getRootNode();
                // ignore internal or interop stack frames
                if (rn.isInternal() || rn.getLanguageInfo() == null) {
                    return 1;
                }
                if (str.length() > 0) {
                    str.append(System.getProperty("line.separator"));
                }
                str.append("Frame: ").append(rn.toString());
                FrameDescriptor frameDescriptor = frame.getFrameDescriptor();
                for (FrameSlot s : frameDescriptor.getSlots()) {
                    str.append(", ").append(s.getIdentifier()).append("=").append(frame.getValue(s));
                }
                return null;
            }
        });
        return str.toString();
    }
}
