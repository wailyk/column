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

import org.apache.asterix.codegen.asterix.map.IUnsafeHashAggregatorFactory;
import org.apache.asterix.codegen.truffle.runtime.aggregation.storage.EmptyHashAggregatorStorage;
import org.apache.asterix.codegen.truffle.runtime.aggregation.storage.IHashAggregatorStorage;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;

public abstract class AbstractAggregator {
    protected IHashAggregatorStorage storage;

    AbstractAggregator(IUnsafeHashAggregatorFactory aggFactory, String aggType) {
        storage = new EmptyHashAggregatorStorage(aggFactory, aggType, null);
    }

    AbstractAggregator(IUnsafeHashAggregatorFactory aggFactory, String aggType, AILResultWriter resultWriter) {
        storage = new EmptyHashAggregatorStorage(aggFactory, aggType, resultWriter);
    }

    public IHashAggregatorStorage getStorage() {
        return storage;
    }

    public void setStorage(IHashAggregatorStorage storage) {
        this.storage = storage;
    }
}
