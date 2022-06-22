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
package org.apache.asterix.codegen.truffle.runtime;

import org.apache.asterix.codegen.truffle.AILException;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.nodes.Node;

public final class AILUndefinedNameException extends AILException {

    private static final long serialVersionUID = 1L;

    @TruffleBoundary
    public static AILUndefinedNameException undefinedFunction(Node location, Object name) {
        throw new AILUndefinedNameException("Undefined function: " + name, location);
    }

    @TruffleBoundary
    public static AILUndefinedNameException undefinedProperty(Node location, Object name) {
        throw new AILUndefinedNameException("Undefined property: " + name, location);
    }

    private AILUndefinedNameException(String message, Node node) {
        super(message, node);
    }
}
