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

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.Frame;
import com.oracle.truffle.api.frame.FrameInstance;
import com.oracle.truffle.api.frame.FrameInstance.FrameAccess;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * This builtin sets the variable named "hello" in the caller frame to the string "world".
 */
@NodeInfo(shortName = "helloEqualsWorld")
public abstract class AILHelloEqualsWorldBuiltin extends AILBuiltinNode {

    @Specialization
    @TruffleBoundary
    public String change() {
        FrameInstance frameInstance = Truffle.getRuntime().getCallerFrame();
        Frame frame = frameInstance.getFrame(FrameAccess.READ_WRITE);
        FrameSlot slot = frame.getFrameDescriptor().findOrAddFrameSlot("hello");
        frame.setObject(slot, "world");
        return "world";
    }
}
