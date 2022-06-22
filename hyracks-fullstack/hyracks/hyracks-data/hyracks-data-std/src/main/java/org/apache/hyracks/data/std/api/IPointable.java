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
package org.apache.hyracks.data.std.api;

/**
 * Point to range over byte array
 */
public interface IPointable extends IValueReference {
    /**
     * Point to the range from position = start with length = length over the byte array bytes
     *
     * @param bytes
     *            the byte array
     * @param start
     *            the start offset
     * @param length
     *            the length of the range
     */
    void set(byte[] bytes, int start, int length);

    /**
     * Point to the same range pointed to by the passed pointer
     *
     * @param pointer
     *            the pointer to the targetted range
     */
    void set(IValueReference pointer);
}
