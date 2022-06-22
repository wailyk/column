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

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.runtime.AILContext;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.source.Source;

/**
 * Builtin function to define (or redefine) functions. The provided source code is parsed the same
 * way as the initial source of the script, so the same syntax applies.
 */
@NodeInfo(shortName = "defineFunction")
public abstract class AILDefineFunctionBuiltin extends AILBuiltinNode {

    @TruffleBoundary
    @Specialization
    public String defineFunction(String code) {
        // @formatter:off
        Source source = Source.newBuilder(AILLanguage.ID, code, "[defineFunction]").build();
        // @formatter:on
        /* The same parsing code as for parsing the initial source. */
        AILContext.get(this).getFunctionRegistry().register(source);

        return code;
    }
}
