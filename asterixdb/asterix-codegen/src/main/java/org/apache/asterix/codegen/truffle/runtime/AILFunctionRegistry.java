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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.parser.AsterixInternalLanguageParser;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.source.Source;

/**
 * Manages the mapping from function names to {@link AILFunction function objects}.
 */
public final class AILFunctionRegistry {

    private final AILLanguage language;
    private final FunctionsObject functionsObject = new FunctionsObject();
    private final Map<Map<String, RootCallTarget>, Void> registeredFunctions = new IdentityHashMap<>();

    public AILFunctionRegistry(AILLanguage language) {
        this.language = language;
    }

    /**
     * Returns the canonical {@link AILFunction} object for the given name. If it does not exist yet,
     * it is created.
     */
    @TruffleBoundary
    public AILFunction lookup(String name, boolean createIfNotPresent) {
        AILFunction result = functionsObject.functions.get(name);
        if (result == null && createIfNotPresent) {
            result = new AILFunction(language, name);
            functionsObject.functions.put(name, result);
        }
        return result;
    }

    /**
     * Associates the {@link AILFunction} with the given name with the given implementation root
     * node. If the function did not exist before, it defines the function. If the function existed
     * before, it redefines the function and the old implementation is discarded.
     */
    AILFunction register(String name, RootCallTarget callTarget) {
        AILFunction result = functionsObject.functions.get(name);
        if (result == null) {
            result = new AILFunction(callTarget);
            functionsObject.functions.put(name, result);
        } else {
            result.setCallTarget(callTarget);
        }
        return result;
    }

    /**
     * Registers a map of functions. The once registered map must not change in order to allow to
     * cache the registration for the entire map. If the map is changed after registration the
     * functions might not get registered.
     */
    @TruffleBoundary
    public void register(Map<String, RootCallTarget> newFunctions) {
        if (registeredFunctions.containsKey(newFunctions)) {
            return;
        }
        for (Map.Entry<String, RootCallTarget> entry : newFunctions.entrySet()) {
            register(entry.getKey(), entry.getValue());
        }
        registeredFunctions.put(newFunctions, null);
    }

    public void register(Source newFunctions) {
        register(AsterixInternalLanguageParser.parseAIL(language, newFunctions));
    }

    public AILFunction getFunction(String name) {
        return functionsObject.functions.get(name);
    }

    /**
     * Returns the sorted list of all functions, for printing purposes only.
     */
    public List<AILFunction> getFunctions() {
        List<AILFunction> result = new ArrayList<>(functionsObject.functions.values());
        Collections.sort(result, Comparator.comparing(AILFunction::toString));
        return result;
    }

    public TruffleObject getFunctionsObject() {
        return functionsObject;
    }

}
