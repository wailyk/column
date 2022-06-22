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
package org.apache.asterix.codegen.truffle.runtime.array.storage;

import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;

import com.oracle.truffle.api.dsl.GenerateUncached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.Node;

public class ArrayStorageNodes {
    @GenerateUncached
    public abstract static class AppendNode extends Node {
        public abstract AbstractArrayStorage execute(AbstractArrayStorage storage, Object value);

        @Specialization
        protected static AbstractArrayStorage add(BooleanArrayStorage storage, boolean value) {
            storage.append(value);
            return storage;
        }

        @Specialization
        protected static AbstractArrayStorage add(LongArrayStorage storage, long value) {
            storage.append(value);
            return storage;
        }

        @Specialization
        protected static AbstractArrayStorage add(DoubleArrayStorage storage, double value) {
            storage.append(value);
            return storage;
        }

        @Specialization
        protected static AbstractArrayStorage add(StringArrayStorage storage, AILStringRuntime value) {
            storage.append(value);
            return storage;
        }
    }

    @GenerateUncached
    public abstract static class GetNode extends Node {

        public abstract Object execute(AbstractArrayStorage storage, long value);

        @Specialization
        protected static boolean get(BooleanArrayStorage storage, long index) {
            return storage.get((int) index);
        }

        @Specialization
        protected static long get(LongArrayStorage storage, long index) {
            return storage.get((int) index);
        }

        @Specialization
        protected static double get(DoubleArrayStorage storage, long index) {
            return storage.get((int) index);
        }

        @Specialization
        protected static AILStringRuntime get(StringArrayStorage storage, long index) {
            return storage.get((int) index);
        }
    }
}
