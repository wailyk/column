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

import static com.oracle.truffle.api.CompilerDirectives.shouldNotReachHere;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.builtins.AILAggregateBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILAppendBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILBuiltinNode;
import org.apache.asterix.codegen.truffle.builtins.AILDefineFunctionBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILEvalBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILFlushBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILGetSizeBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILHasSizeBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILHelloEqualsWorldBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILIsExecutableBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILIsInstanceBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILIsNullBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILJavaTypeBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILNanoTimeBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILNewAggregatorBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILNewTopKFactory;
import org.apache.asterix.codegen.truffle.builtins.AILOneZeroFactory;
import org.apache.asterix.codegen.truffle.builtins.AILPrintlnBuiltin;
import org.apache.asterix.codegen.truffle.builtins.AILPrintlnBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILReadlnBuiltin;
import org.apache.asterix.codegen.truffle.builtins.AILReadlnBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILStackTraceBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.AILWrapPrimitiveBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.numeric.AILAbsBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.numeric.AILCosBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.numeric.AILCoshBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.numeric.AILFloorBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.numeric.AILSinBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.numeric.AILSinhBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.numeric.AILSqrtBuiltinFactory;
import org.apache.asterix.codegen.truffle.builtins.string.AILStringLengthFactory;
import org.apache.asterix.codegen.truffle.builtins.string.AILStringLowercaseFactory;
import org.apache.asterix.codegen.truffle.nodes.AILExpressionNode;
import org.graalvm.polyglot.Context;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.TruffleLanguage.ContextReference;
import com.oracle.truffle.api.TruffleLanguage.Env;
import com.oracle.truffle.api.dsl.NodeFactory;
import com.oracle.truffle.api.instrumentation.AllocationReporter;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.source.Source;

/**
 * The run-time state of SL during execution. The context is created by the {@link AILLanguage}. It
 * is used, for example, by {@link AILBuiltinNode#getContext() builtin functions}.
 * <p>
 * It would be an error to have two different context instances during the execution of one script.
 * However, if two separate scripts run in one Java VM at the same time, they have a different
 * context. Therefore, the context is not a singleton.
 */
public final class AILContext {

    private final AILLanguage language;
    @CompilationFinal
    private Env env;
    private final BufferedReader input;
    private final PrintWriter output;
    private final AILFunctionRegistry functionRegistry;
    private final AllocationReporter allocationReporter;

    public AILContext(AILLanguage language, TruffleLanguage.Env env,
            List<NodeFactory<? extends AILBuiltinNode>> externalBuiltins) {
        this.env = env;
        this.input = new BufferedReader(new InputStreamReader(env.in()));
        this.output = new PrintWriter(env.out(), true);
        this.language = language;
        this.allocationReporter = env.lookup(AllocationReporter.class);
        this.functionRegistry = new AILFunctionRegistry(language);
        installBuiltins();
        for (NodeFactory<? extends AILBuiltinNode> builtin : externalBuiltins) {
            installBuiltin(builtin);
        }
    }

    /**
     * Patches the {@link AILContext} to use a new {@link Env}. The method is called during the
     * native image execution as a consequence of {@link Context#create(java.lang.String...)}.
     *
     * @param newEnv the new {@link Env} to use.
     * @see TruffleLanguage#patchContext(Object, Env)
     */
    public void patchContext(Env newEnv) {
        this.env = newEnv;
    }

    /**
     * Return the current Truffle environment.
     */
    public Env getEnv() {
        return env;
    }

    /**
     * Returns the default input, i.e., the source for the {@link AILReadlnBuiltin}. To allow unit
     * testing, we do not use {@link System#in} directly.
     */
    public BufferedReader getInput() {
        return input;
    }

    /**
     * The default default, i.e., the output for the {@link AILPrintlnBuiltin}. To allow unit
     * testing, we do not use {@link System#out} directly.
     */
    public PrintWriter getOutput() {
        return output;
    }

    /**
     * Returns the registry of all functions that are currently defined.
     */
    public AILFunctionRegistry getFunctionRegistry() {
        return functionRegistry;
    }

    /**
     * Adds all builtin functions to the {@link AILFunctionRegistry}. This method lists all
     * {@link AILBuiltinNode builtin implementation classes}.
     */
    private void installBuiltins() {
        installBuiltin(AILReadlnBuiltinFactory.getInstance());
        installBuiltin(AILPrintlnBuiltinFactory.getInstance());
        installBuiltin(AILNanoTimeBuiltinFactory.getInstance());
        installBuiltin(AILDefineFunctionBuiltinFactory.getInstance());
        installBuiltin(AILStackTraceBuiltinFactory.getInstance());
        installBuiltin(AILHelloEqualsWorldBuiltinFactory.getInstance());
        installBuiltin(AILEvalBuiltinFactory.getInstance());
        installBuiltin(AILGetSizeBuiltinFactory.getInstance());
        installBuiltin(AILHasSizeBuiltinFactory.getInstance());
        installBuiltin(AILIsExecutableBuiltinFactory.getInstance());
        installBuiltin(AILIsNullBuiltinFactory.getInstance());
        installBuiltin(AILWrapPrimitiveBuiltinFactory.getInstance());
        installBuiltin(AILIsInstanceBuiltinFactory.getInstance());
        installBuiltin(AILJavaTypeBuiltinFactory.getInstance());
        installBuiltin(AILOneZeroFactory.getInstance());
        installBuiltin(AILAppendBuiltinFactory.getInstance());
        installBuiltin(AILFlushBuiltinFactory.getInstance());
        installBuiltin(AILNewTopKFactory.getInstance());
        installBuiltin(AILNewAggregatorBuiltinFactory.getInstance());
        installBuiltin(AILAggregateBuiltinFactory.getInstance());
        installBuiltin(AILStringLengthFactory.getInstance());
        installBuiltin(AILStringLowercaseFactory.getInstance());
        installBuiltin(AILFloorBuiltinFactory.getInstance());
        installBuiltin(AILAbsBuiltinFactory.getInstance());
        installBuiltin(AILCosBuiltinFactory.getInstance());
        installBuiltin(AILCoshBuiltinFactory.getInstance());
        installBuiltin(AILSinBuiltinFactory.getInstance());
        installBuiltin(AILSinhBuiltinFactory.getInstance());
        installBuiltin(AILSqrtBuiltinFactory.getInstance());
    }

    public void installBuiltin(NodeFactory<? extends AILBuiltinNode> factory) {
        /* Register the builtin function in our function registry. */
        RootCallTarget target = language.lookupBuiltin(factory);
        String rootName = target.getRootNode().getName();
        getFunctionRegistry().register(rootName, target);
    }

    /*
     * Methods for object creation / object property access.
     */
    public AllocationReporter getAllocationReporter() {
        return allocationReporter;
    }

    public RootCallTarget createFunction(AILExpressionNode expressionNode, String name) {
        return language.createRootCallTarget(expressionNode, name);
    }

    /*
     * Methods for language interoperability.
     */
    public static Object fromForeignValue(Object a) {
        if (a instanceof Long || a instanceof String || a instanceof Boolean) {
            return a;
        } else if (a instanceof Character) {
            return fromForeignCharacter((Character) a);
        } else if (a instanceof Number) {
            return fromForeignNumber(a);
        } else if (a instanceof TruffleObject) {
            return a;
        } else if (a instanceof AILContext) {
            return a;
        }
        throw shouldNotReachHere("Value is not a truffle value.");
    }

    @TruffleBoundary
    private static long fromForeignNumber(Object a) {
        return ((Number) a).longValue();
    }

    @TruffleBoundary
    private static String fromForeignCharacter(char c) {
        return String.valueOf(c);
    }

    public CallTarget parse(Source source) {
        return env.parsePublic(source);
    }

    /**
     * Returns an object that contains bindings that were exported across all used languages. To
     * read or write from this object the {@link TruffleObject interop} API can be used.
     */
    public TruffleObject getPolyglotBindings() {
        return (TruffleObject) env.getPolyglotBindings();
    }

    private static final ContextReference<AILContext> REFERENCE = ContextReference.create(AILLanguage.class);

    public static AILContext get(Node node) {
        return REFERENCE.get(node);
    }
}
