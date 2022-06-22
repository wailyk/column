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

import org.apache.asterix.codegen.truffle.AILException;
import org.apache.asterix.codegen.truffle.runtime.AILContext;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Builtin that allows to lookup a Java type.
 */
@NodeInfo(shortName = "java")
public abstract class AILJavaTypeBuiltin extends AILBuiltinNode {

    @Specialization
    public Object doLookup(Object symbolName, @CachedLibrary(limit = "3") InteropLibrary interop) {
        try {
            /*
             * This is the entry point to Java host interoperability. The return value of
             * lookupHostSymbol implements the interop contracts. So we can use Java for things that
             * are expressible also in SL. Like function calls on objects.
             */
            return AILContext.get(this).getEnv().lookupHostSymbol(interop.asString(symbolName));
        } catch (UnsupportedMessageException e) {
            throw new AILException(
                    "The java builtin expected a String argument, but a non-string argument was provided.", this);
        }
    }

}
