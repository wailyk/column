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

package org.apache.asterix.om.util.container;

/**
 * A reusable object pool interface.
 */
public interface IObjectPool<E, T> {

    /**
     * Give client an E instance
     *
     * @param arg
     *            the argument to create E
     * @return an E instance
     */
    E allocate(T arg);

    /**
     * Mark all instances in the pool as unused
     */
    void reset();

    /**
     * Frees the argument element in the pool and makes it available again.
     *
     * @param element instance to free.
     * @return true if the element is marked available in the pool. Otherwise, false.
     */
    boolean free(E element);
}
