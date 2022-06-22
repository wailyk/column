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

import org.apache.asterix.codegen.asterix.map.AbstractUnsafeHashAggregator;
import org.apache.asterix.codegen.asterix.map.IUnsafeAggregator;
import org.apache.asterix.codegen.asterix.map.IUnsafeHashAggregatorFactory;
import org.apache.asterix.codegen.asterix.map.UnsafeHashAggregator;
import org.apache.asterix.codegen.asterix.map.entry.IUnsafeMapResultAppender;
import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.runtime.result.AILResultWriter;
import org.apache.hyracks.unsafe.entry.IEntryComparator;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

@ExportLibrary(InteropLibrary.class)
public class AILGeneralAggregator extends AbstractAggregator implements TruffleObject {

    public AILGeneralAggregator(String aggType, long budget, AILResultWriter resultWriter) {
        super(new GeneralAggregatorFactory(budget), aggType, resultWriter);
    }

    @ExportMessage
    Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects) {
        return "General Aggregator";
    }

    @ExportMessage
    boolean hasLanguage() {
        return true;
    }

    @ExportMessage
    Class<? extends TruffleLanguage<?>> getLanguage() {
        return AILLanguage.class;
    }

    private static class GeneralAggregatorFactory implements IUnsafeHashAggregatorFactory {
        private final long budget;

        GeneralAggregatorFactory(long budget) {
            this.budget = budget;
        }

        @Override
        public AbstractUnsafeHashAggregator createInstance(IUnsafeAggregator aggregator,
                IUnsafeMapResultAppender appender, IEntryComparator keyComparator, IEntryComparator valueComparator) {
            return new UnsafeHashAggregator(aggregator, appender, keyComparator, budget);
        }
    }
}
