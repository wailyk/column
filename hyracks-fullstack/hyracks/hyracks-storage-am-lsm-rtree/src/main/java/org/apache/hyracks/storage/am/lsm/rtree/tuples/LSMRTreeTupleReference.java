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

package org.apache.hyracks.storage.am.lsm.rtree.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleReference;

public class LSMRTreeTupleReference extends RTreeTypeAwareTupleReference implements ILSMTreeTupleReference {

    public LSMRTreeTupleReference(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits) {
        super(typeTraits, nullTypeTraits);
    }

    @Override
    protected int getNullFlagsBytes() {
        // +1.0 is for matter/antimatter bit.
        return BitOperationUtils.getFlagBytes(fieldCount + 1);
    }

    @Override
    public boolean isAntimatter() {
        // Check antimatter bit.
        return BitOperationUtils.getBit(buf, tupleStartOff, ANTIMATTER_BIT_OFFSET);
    }

    public int getTupleStart() {
        return tupleStartOff;
    }

    @Override
    protected int getAdjustedFieldIdx(int fieldIdx) {
        // 1 for antimatter
        return fieldIdx + 1;
    }
}
