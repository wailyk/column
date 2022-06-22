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
package org.apache.asterix.codegen.truffle.nodes.controlflow;

import com.oracle.truffle.api.nodes.ControlFlowException;

/**
 * Exception thrown by the {@link AILBreakNode break statement} and caught by the {@link AILWhileNode
 * loop statement}. Since the exception is stateless, i.e., has no instance fields, we can use a
 * {@link #SINGLETON} to avoid memory allocation during interpretation.
 */
public final class AILBreakException extends ControlFlowException {

    public static final AILBreakException SINGLETON = new AILBreakException();

    private static final long serialVersionUID = -91013036379258890L;

    /* Prevent instantiation from outside. */
    private AILBreakException() {
    }
}
