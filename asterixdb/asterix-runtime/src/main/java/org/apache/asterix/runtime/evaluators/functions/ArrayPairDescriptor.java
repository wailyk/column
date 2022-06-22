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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.IOException;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * array_pair(arg1) takes 1 argument, an array, and returns an array with all possible permutations between the
 * array's items.
 */

@MissingNullInOutFunction
public class ArrayPairDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType inputListType;
    private IAType outputListType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayPairDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.LISTIFY_INFERER;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_PAIR;
    }

    @Override
    public void setImmutableStates(Object... states) {
        inputListType = (IAType) states[0];
        outputListType = (IAType) states[1];
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ArrayPairEval(args, ctx, inputListType, outputListType);
            }
        };
    }

    private static class ArrayPairEval extends AbstractArrayProcessEval {
        private final IAType outputListType;
        private final IAsterixListBuilder orderedListBuilder;
        private final IAsterixListBuilder unorderedListBuilder;
        private final ArrayBackedValueStorage pairStorage;

        ArrayPairEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, IAType inputListType,
                IAType outputListType) throws HyracksDataException {
            super(args, ctx, inputListType);
            this.outputListType = outputListType;
            orderedListBuilder = new OrderedListBuilder();
            unorderedListBuilder = new UnorderedListBuilder();
            pairStorage = new ArrayBackedValueStorage();
        }

        @Override
        protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder) throws IOException {
            IAsterixListBuilder pairBuilder =
                    listAccessor.getListType() == ATypeTag.ARRAY ? orderedListBuilder : unorderedListBuilder;
            if (listAccessor.size() == 1) {
                listBuilder.addItem(getItem(listAccessor, 0));
            }
            for (int i = 0; i < listAccessor.size(); i++) {
                IPointable iValue = getItem(listAccessor, i);
                for (int j = i + 1; j < listAccessor.size(); j++) {
                    IPointable jValue = getItem(listAccessor, j);

                    orderedListBuilder.reset((AbstractCollectionType) outputListType);
                    pairBuilder.addItem(iValue);
                    pairBuilder.addItem(jValue);

                    pairStorage.reset();
                    pairBuilder.write(pairStorage.getDataOutput(), true);
                    listBuilder.addItem(pairStorage);
                }
            }
        }

        private IPointable getItem(ListAccessor listAccessor, int index) throws IOException {
            IPointable item = pointableAllocator.allocateEmpty();
            ArrayBackedValueStorage storage = null;
            if (!listAccessor.itemsAreSelfDescribing()) {
                storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
            }
            listAccessor.getOrWriteItem(index, item, storage);
            return item;
        }

    }
}