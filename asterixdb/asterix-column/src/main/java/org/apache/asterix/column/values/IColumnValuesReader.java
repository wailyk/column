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
package org.apache.asterix.column.values;

import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.value.IValueReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IColumnValuesReader extends IValueReader, Comparable<IColumnValuesReader> {

    void reset(AbstractBytesInputStream in, int tupleCount) throws HyracksDataException;

    /* ***********************
     * Iteration functions
     * ***********************
     */

    boolean next() throws HyracksDataException;

    /* ***********************
     * Information functions
     * ***********************
     */
    ATypeTag getTypeTag();

    int getColumnIndex();

    int getMaxLevel();

    int getLevel();

    boolean isMissing();

    boolean isNull();

    boolean isValue();

    boolean isRepeated();

    boolean isDelimiter();

    int getDelimiterIndex();

    boolean isLastDelimiter();

    /* ***********************
     * Write function
     * ***********************
     */

    /**
     * Write the content of reader to the writer
     *
     * @param writer   to which is the content of this reader is written to
     * @param callNext should call next on write
     */
    void write(IColumnValuesWriter writer, boolean callNext) throws HyracksDataException;

    /**
     * Write the content of reader to the writer
     *
     * @param writer to which is the content of this reader is written to
     * @param count  number of values to write
     */
    void write(IColumnValuesWriter writer, int count) throws HyracksDataException;

    void skip(int count) throws HyracksDataException;
}
