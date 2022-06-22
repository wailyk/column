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
package org.apache.asterix.codegen.truffle.nodes;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.runtime.AILUndefinedNameException;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.ArityException;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.interop.UnsupportedTypeException;
import com.oracle.truffle.api.nodes.RootNode;

public class ExternalRootNode extends RootNode {
    private final Object call;
    @Child
    private InteropLibrary library;

    private final String name;

    public ExternalRootNode(Object call, String name) {
        super(AILLanguage.get(null));
        this.call = call;
        library = InteropLibrary.getFactory().createDispatched(3);
        this.name = name;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        try {
            return library.execute(call);
        } catch (ArityException | UnsupportedTypeException | UnsupportedMessageException e) {
            /* Execute was not successful. */
            throw AILUndefinedNameException.undefinedFunction(this, call);
        }
    }

    @Override
    public String getName() {
        return name;
    }
}
