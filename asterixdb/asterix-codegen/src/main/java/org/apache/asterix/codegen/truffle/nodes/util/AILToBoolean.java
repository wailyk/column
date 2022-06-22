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
package org.apache.asterix.codegen.truffle.nodes.util;

import static com.oracle.truffle.api.CompilerDirectives.shouldNotReachHere;

import org.apache.asterix.codegen.truffle.nodes.AILExpressionNode;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;

@NodeChild
public abstract class AILToBoolean extends AILExpressionNode {
    static final int LIMIT = 5;

    @Specialization
    protected boolean fromBoolean(boolean value) {
        return value;
    }

    @Specialization(limit = "LIMIT")
    public static boolean fromGeneric(Object value, @CachedLibrary("value") InteropLibrary interop) {
        try {
            if (interop.isBoolean(value)) {
                return interop.asBoolean(value);
            }
        } catch (UnsupportedMessageException e) {
            throw shouldNotReachHere(e);
        }
        return false;
    }
}
