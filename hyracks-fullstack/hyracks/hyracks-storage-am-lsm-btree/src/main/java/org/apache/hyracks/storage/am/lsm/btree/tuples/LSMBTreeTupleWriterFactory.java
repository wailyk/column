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

package org.apache.hyracks.storage.am.lsm.btree.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriter;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;

public class LSMBTreeTupleWriterFactory extends BTreeTypeAwareTupleWriterFactory {

    private static final long serialVersionUID = 1L;
    private final int numKeyFields;
    private final boolean isAntimatter;

    public LSMBTreeTupleWriterFactory(ITypeTraits[] typeTraits, int numKeyFields, boolean isAntimatter,
            boolean updateAware, ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector) {
        super(typeTraits, updateAware, nullTypeTraits, nullIntrospector);
        this.numKeyFields = numKeyFields;
        this.isAntimatter = isAntimatter;
    }

    @Override
    public BTreeTypeAwareTupleWriter createTupleWriter() {
        return new LSMBTreeTupleWriter(typeTraits, numKeyFields, isAntimatter, updateAware, nullTypeTraits,
                nullIntrospector);
    }

}
