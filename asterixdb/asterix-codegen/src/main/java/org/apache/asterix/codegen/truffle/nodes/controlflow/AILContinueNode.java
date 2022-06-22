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
package org.apache.asterix.codegen.truffle.nodes.controlflow;

import org.apache.asterix.codegen.truffle.nodes.AILStatementNode;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Implementation of the SL continue statement. We need to unwind an unknown number of interpreter
 * frames that are between this {@link AILContinueNode} and the {@link AILWhileNode} of the loop we
 * are continuing. This is done by throwing an {@link AILContinueException exception} that is caught
 * by the {@link AILWhileNode#executeVoid loop node}.
 */
@NodeInfo(shortName = "continue", description = "The node implementing a continue statement")
public final class AILContinueNode extends AILStatementNode {

    @Override
    public void executeVoid(VirtualFrame frame) {
        throw AILContinueException.SINGLETON;
    }
}
