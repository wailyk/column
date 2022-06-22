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
package org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.string;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.AbstractBuiltinFunction;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.ICodeNodeBuiltinFunctionFactory;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class StringLowercaseBuiltinNode extends AbstractBuiltinFunction {
    private static final long serialVersionUID = -7635100797874331207L;
    public static final ICodeNodeBuiltinFunctionFactory FACTORY = StringLowercaseBuiltinNode::new;

    private StringLowercaseBuiltinNode(SourceLocation sourceLocation, ICodeNode... args) {
        super(sourceLocation, args);
    }

    @Override
    protected String getRawName() {
        return BuiltinFunctions.STRING_LOWERCASE.getName();
    }
}
