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
package org.apache.asterix.optimizer.rules.codegen.node.expression.builtin;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.numeric.NumericAbsBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.numeric.NumericCosBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.numeric.NumericCoshBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.numeric.NumericFloorBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.numeric.NumericSinBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.numeric.NumericSinhBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.numeric.NumericSqrtBuiltinCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.string.StringLengthBuiltinNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.string.StringLowercaseBuiltinNode;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class BuiltinFunctionProvider {
    private static final Map<FunctionIdentifier, ICodeNodeBuiltinFunctionFactory> FUNCTION_FACTORY_MAP =
            new HashMap<>();

    private BuiltinFunctionProvider() {
    }

    static {
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.IS_UNKNOWN, IsUnknownBuiltinFunctionCodeNode.FACTORY);
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.STRING_LOWERCASE, StringLowercaseBuiltinNode.FACTORY);
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.STRING_LENGTH, StringLengthBuiltinNode.FACTORY);
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.NUMERIC_FLOOR, NumericFloorBuiltinCodeNode.FACTORY);
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.NUMERIC_ABS, NumericAbsBuiltinCodeNode.FACTORY);
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.NUMERIC_COS, NumericCosBuiltinCodeNode.FACTORY);
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.NUMERIC_COSH, NumericCoshBuiltinCodeNode.FACTORY);
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.NUMERIC_SQRT, NumericSqrtBuiltinCodeNode.FACTORY);
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.NUMERIC_SIN, NumericSinBuiltinCodeNode.FACTORY);
        FUNCTION_FACTORY_MAP.put(BuiltinFunctions.NUMERIC_SINH, NumericSinhBuiltinCodeNode.FACTORY);
    }

    public static AbstractBuiltinFunction createFunction(FunctionIdentifier fid, SourceLocation sourceLocation,
            ICodeNode... args) throws CompilationException {
        if (!FUNCTION_FACTORY_MAP.containsKey(fid)) {
            throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, sourceLocation, fid.getName());
        }
        return FUNCTION_FACTORY_MAP.get(fid).create(sourceLocation, args);
    }
}
