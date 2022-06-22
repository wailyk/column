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
package org.apache.asterix.codegen.truffle.runtime.aggregation;

import org.apache.asterix.codegen.truffle.runtime.AILMissingRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILNullRuntime;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.codegen.truffle.runtime.aggregation.storage.EmptyHashAggregatorStorage;
import org.apache.asterix.codegen.truffle.runtime.aggregation.storage.HashAggregatorStorageNodes.AddNode;
import org.apache.asterix.codegen.truffle.runtime.aggregation.storage.HashTableType;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.GenerateUncached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.Node;

public class AILAggregatorNodes {
    @GenerateUncached
    public abstract static class AggregateNode extends Node {

        public abstract AbstractAggregator execute(AbstractAggregator aggregator, Object key, long value);

        public abstract AbstractAggregator execute(AbstractAggregator aggregator, Object key, Object value);

        @Specialization(guards = "isEmpty(aggregator)")
        protected static AbstractAggregator fromEmpty(AbstractAggregator aggregator, Object key, Object value,
                @Cached AggregateNode aggregateNode) {
            CompilerDirectives.transferToInterpreter();
            EmptyHashAggregatorStorage emptyAggregator = (EmptyHashAggregatorStorage) aggregator.getStorage();
            aggregator.setStorage(emptyAggregator.specialize(key, value));
            aggregateNode.execute(aggregator, key, value);
            return aggregator;
        }

        @Specialization
        protected static AbstractAggregator add(AbstractAggregator aggregator, AILMissingRuntime key, long value,
                @Cached AddNode addNode) {
            return aggregator;
        }

        @Specialization
        protected static AbstractAggregator add(AbstractAggregator aggregator, AILNullRuntime key, long value,
                @Cached AddNode addNode) {
            addNode.execute(aggregator.getStorage(), key, value);
            return aggregator;
        }

        @Specialization
        protected static AbstractAggregator add(AbstractAggregator aggregator, long key, long value,
                @Cached AddNode addNode) {
            addNode.execute(aggregator.getStorage(), key, value);
            return aggregator;
        }

        @Specialization
        protected static AbstractAggregator add(AbstractAggregator aggregator, double key, long value,
                @Cached AddNode addNode) {
            addNode.execute(aggregator.getStorage(), key, value);
            return aggregator;
        }

        @Specialization
        protected static AbstractAggregator add(AbstractAggregator aggregator, AILStringRuntime key, long value,
                @Cached AddNode addNode) {
            addNode.execute(aggregator.getStorage(), key, value);
            return aggregator;
        }

        protected static boolean isEmpty(AbstractAggregator aggregator) {
            return aggregator.getStorage().getType() == HashTableType.EMPTY;
        }
    }
}
