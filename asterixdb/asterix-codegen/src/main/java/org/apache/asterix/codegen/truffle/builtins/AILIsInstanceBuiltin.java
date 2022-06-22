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

import static com.oracle.truffle.api.CompilerDirectives.shouldNotReachHere;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Built-in function that returns true if the given operand is of a given meta-object. Meta-objects
 * may be values of the current or a foreign value.
 */
@NodeInfo(shortName = "isInstance")
@SuppressWarnings("unused")
public abstract class AILIsInstanceBuiltin extends AILBuiltinNode {

    @Specialization(limit = "3", guards = "metaLib.isMetaObject(metaObject)")
    public Object doDefault(Object metaObject, Object value, @CachedLibrary("metaObject") InteropLibrary metaLib) {
        try {
            return metaLib.isMetaInstance(metaObject, value);
        } catch (UnsupportedMessageException e) {
            throw shouldNotReachHere(e);
        }
    }

}
