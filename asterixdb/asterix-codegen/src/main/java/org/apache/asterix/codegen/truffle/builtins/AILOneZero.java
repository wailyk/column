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

import org.apache.asterix.codegen.truffle.runtime.AILMissingRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILNullRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.om.types.ATypeTag;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Return 0 if the argument value is {@link ATypeTag#NULL} OR {@link ATypeTag#MISSING}, 1 otherwise
 */
@NodeInfo(shortName = "oneZero")
public abstract class AILOneZero extends AILBuiltinNode {

    @Specialization
    public long oneZero(boolean value) {
        return 1L;
    }

    @Specialization
    public long oneZero(long value) {
        return 1L;
    }

    @Specialization
    public long oneZero(double value) {
        return 1L;
    }

    @Specialization
    public long oneZero(AILStringRuntime value) {
        return 1L;
    }

    @Specialization
    public long oneZero(AILNullRuntime value) {
        return 0L;
    }

    @Specialization
    public long oneZero(AILMissingRuntime value) {
        return 0L;
    }
}
