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

import org.apache.asterix.codegen.asterix.map.AbstractUnsafeHashAggregator;
import org.apache.asterix.codegen.asterix.map.IUnsafeAggregator;
import org.apache.asterix.codegen.asterix.map.IUnsafeHashAggregatorFactory;
import org.apache.asterix.codegen.asterix.map.entry.IUnsafeMapResultAppender;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;
import org.apache.hyracks.unsafe.entry.IEntry;
import org.apache.hyracks.unsafe.entry.IEntryComparator;

public abstract class AbstractHashAggregatorStorage implements IHashAggregatorStorage, IUnsafeMapResultAppender {
    private final AbstractUnsafeHashAggregator computer;
    private final AILResultWriter resultWriter;

    AbstractHashAggregatorStorage(IUnsafeHashAggregatorFactory aggregatorFactory, AILResultWriter resultWriter,
            IUnsafeAggregator aggregator, IEntryComparator keyComparator, IEntryComparator valueComparator) {
        this.computer = aggregatorFactory.createInstance(aggregator, this, keyComparator, valueComparator);
        this.resultWriter = resultWriter;
    }

    @Override
    public final void append(AILResultWriter resultWriter) {
        computer.append(resultWriter);
    }

    final void aggregate(IEntry key, IEntry value) {
        if (!computer.aggregate(key, value)) {
            computer.append(resultWriter);
            computer.aggregate(key, value);
        }
    }
}
