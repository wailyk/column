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

import org.apache.asterix.codegen.truffle.runtime.AILContext;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.source.Source;

/**
 * Builtin function to evaluate source code in any supported language.
 * <p>
 * The call target is cached against the language id and the source code, so that if they are the
 * same each time then a direct call will be made to a cached AST, allowing it to be compiled and
 * possibly inlined.
 */
@NodeInfo(shortName = "eval")
@SuppressWarnings("unused")
public abstract class AILEvalBuiltin extends AILBuiltinNode {

    static final int LIMIT = 2;

    @Specialization(guards = { "stringsEqual(cachedId, id)", "stringsEqual(cachedCode, code)" }, limit = "LIMIT")
    public Object evalCached(String id, String code, @Cached("id") String cachedId, @Cached("code") String cachedCode,
            @Cached("create(parse(id, code))") DirectCallNode callNode) {
        return callNode.call(new Object[] {});
    }

    @TruffleBoundary
    @Specialization(replaces = "evalCached")
    public Object evalUncached(String id, String code) {
        return parse(id, code).call();
    }

    protected CallTarget parse(String id, String code) {
        final Source source = Source.newBuilder(id, code, "(eval)").build();
        return AILContext.get(this).parse(source);
    }

    /* Work around findbugs warning in generate code. */
    protected static boolean stringsEqual(String a, String b) {
        return a.equals(b);
    }
}
