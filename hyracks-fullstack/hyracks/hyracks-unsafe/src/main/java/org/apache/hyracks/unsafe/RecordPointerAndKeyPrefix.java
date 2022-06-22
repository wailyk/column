/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hyracks.unsafe;

/**
 * Repurposed from Apache Spark
 * <a href="https://github.com/apache/spark/blob/6b5a1f9df28262fa90d28dc15af67e8a37a9efcf/core/src/main/java/org/apache/spark/unsafe/map/BytesToBytesMap.java"></a>
 */
final class RecordPointerAndKeyPrefix {
    /**
     * A pointer to a record; see {@link org.apache.spark.memory.TaskMemoryManager} for a
     * description of how these addresses are encoded.
     */
    public long recordPointer;

    /**
     * A key prefix, for use in comparisons.
     */
    public long keyPrefix;
}