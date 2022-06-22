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
package org.apache.asterix.codegen.truffle.runtime.aggregation.storage;

import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;

import com.oracle.truffle.api.dsl.GenerateUncached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.Node;

public class HashAggregatorStorageNodes {

    @GenerateUncached
    public abstract static class AddNode extends Node {
        public abstract IHashAggregatorStorage execute(IHashAggregatorStorage storage, long key, long value);

        public abstract IHashAggregatorStorage execute(IHashAggregatorStorage storage, Object key, long value);

        @Specialization
        protected static IHashAggregatorStorage add(StringToLongHashAggregatorStorage storage, AILStringRuntime key,
                long value) {
            storage.add(key, value);
            return storage;
        }

        @Specialization
        protected static IHashAggregatorStorage add(DoubleToLongHashAggregatorStorage storage, double key, long value) {
            storage.add(key, value);
            return storage;
        }

        @Specialization
        protected static IHashAggregatorStorage add(LongToLongHashAggregatorStorage storage, long key, long value) {
            storage.add(key, value);
            return storage;
        }
    }
}
