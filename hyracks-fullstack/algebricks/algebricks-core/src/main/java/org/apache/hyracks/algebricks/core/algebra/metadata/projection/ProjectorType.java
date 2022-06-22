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
package org.apache.hyracks.algebricks.core.algebra.metadata.projection;

import org.apache.hyracks.storage.common.projection.ICodeGenerationExecutor;
import org.apache.hyracks.storage.common.projection.ITupleProjector;

public enum ProjectorType {
    /**
     * The default projection type, which uses Hyracks query processor
     *
     * @see ITupleProjector
     */
    INTERPRETED,
    /**
     * Fused Hyracks operators using an application language, which is then compiled and executed
     *
     * @see ICodeGenerationExecutor
     */
    COMPILED
}
