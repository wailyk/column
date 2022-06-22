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
import org.apache.asterix.codegen.truffle.runtime.aggregation.AILGeneralAggregator;
import org.apache.asterix.codegen.truffle.runtime.aggregation.AILMinMaxTopKAggregator;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "append")
public abstract class AILAppendBuiltin extends AILBuiltinNode {
    @Specialization
    @TruffleBoundary
    public Object append(AILResultWriter appender, AILMissingRuntime value) {
        appender.appendMissing();
        return appender;
    }

    @Specialization
    @TruffleBoundary
    public Object append(AILResultWriter appender, AILNullRuntime value) {
        appender.appendNull();
        return appender;
    }

    @Specialization
    @TruffleBoundary
    public Object append(AILResultWriter appender, long value) {
        appender.append(value);
        return appender;
    }

    @Specialization
    @TruffleBoundary
    public Object append(AILResultWriter appender, double value) {
        appender.append(value);
        return appender;
    }

    @Specialization
    @TruffleBoundary
    public Object append(AILResultWriter appender, AILStringRuntime value) {
        appender.append(value);
        return appender;
    }

    @Specialization
    @TruffleBoundary
    public Object append(AILResultWriter appender, AILMinMaxTopKAggregator value) {
        value.getStorage().append(appender);
        return appender;
    }

    @Specialization
    @TruffleBoundary
    public Object append(AILResultWriter appender, AILGeneralAggregator value) {
        value.getStorage().append(appender);
        return appender;
    }
}
