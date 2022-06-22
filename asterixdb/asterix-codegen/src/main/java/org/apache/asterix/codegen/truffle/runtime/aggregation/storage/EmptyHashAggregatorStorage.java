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

import org.apache.asterix.codegen.asterix.map.IUnsafeHashAggregatorFactory;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;

public class EmptyHashAggregatorStorage implements IHashAggregatorStorage {
    private final String aggType;
    private final AILResultWriter resultWriter;
    private final IUnsafeHashAggregatorFactory aggFactory;

    public EmptyHashAggregatorStorage(IUnsafeHashAggregatorFactory aggFactory, String aggType,
            AILResultWriter resultWriter) {
        this.aggFactory = aggFactory;
        this.aggType = aggType;
        this.resultWriter = resultWriter;
    }

    @Override
    public void append(AILResultWriter resultWriter) {
        //NoOp
    }

    public AbstractHashAggregatorStorage specialize(Object key, Object value) {
        if (key instanceof AILStringRuntime && value instanceof Long) {
            return new StringToLongHashAggregatorStorage(aggFactory, aggType, resultWriter);
        } else if (key instanceof Long && value instanceof Long) {
            return new LongToLongHashAggregatorStorage(aggFactory, aggType, resultWriter);
        } else if (key instanceof Double && value instanceof Long) {
            return new DoubleToLongHashAggregatorStorage(aggFactory, aggType, resultWriter);
        }
        return null;
    }

    @Override
    public HashTableType getType() {
        return HashTableType.EMPTY;
    }
}
