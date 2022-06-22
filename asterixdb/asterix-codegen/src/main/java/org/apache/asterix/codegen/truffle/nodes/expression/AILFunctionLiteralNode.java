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
package org.apache.asterix.codegen.truffle.nodes.expression;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.nodes.AILExpressionNode;
import org.apache.asterix.codegen.truffle.runtime.AILContext;
import org.apache.asterix.codegen.truffle.runtime.AILFunction;
import org.apache.asterix.codegen.truffle.runtime.AILFunctionRegistry;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Constant literal for a {@link AILFunction function} value, created when a function name occurs as
 * a literal in SL source code. Note that function redefinition can change the {@link CallTarget
 * call target} that is executed when calling the function, but the {@link AILFunction} for a name
 * never changes. This is guaranteed by the {@link AILFunctionRegistry}.
 */
@NodeInfo(shortName = "func")
public final class AILFunctionLiteralNode extends AILExpressionNode {

    /** The name of the function. */
    private final String functionName;

    /**
     * The resolved function. During parsing (in the constructor of this node), we do not have the
     * {@link AILContext} available yet, so the lookup can only be done at {@link #executeGeneric
     * first execution}. The {@link CompilationFinal} annotation ensures that the function can still
     * be constant folded during compilation.
     */
    @CompilationFinal
    private AILFunction cachedFunction;

    public AILFunctionLiteralNode(String functionName) {
        this.functionName = functionName;
    }

    @Override
    public AILFunction executeGeneric(VirtualFrame frame) {
        AILLanguage l = AILLanguage.get(this);
        CompilerAsserts.partialEvaluationConstant(l);

        AILFunction function;
        if (l.isSingleContext()) {
            function = this.cachedFunction;
            if (function == null) {
                /* We are about to change a @CompilationFinal field. */
                CompilerDirectives.transferToInterpreterAndInvalidate();
                /* First execution of the node: lookup the function in the function registry. */
                this.cachedFunction = function = AILContext.get(this).getFunctionRegistry().lookup(functionName, true);
            }
        } else {
            /*
             * We need to rest the cached function otherwise it might cause a memory leak.
             */
            if (this.cachedFunction != null) {
                CompilerDirectives.transferToInterpreterAndInvalidate();
                this.cachedFunction = null;
            }
            // in the multi-context case we are not allowed to store
            // SLFunction objects in the AST. Instead we always perform the lookup in the hash map.
            function = AILContext.get(this).getFunctionRegistry().lookup(functionName, true);
        }
        return function;
    }

}
