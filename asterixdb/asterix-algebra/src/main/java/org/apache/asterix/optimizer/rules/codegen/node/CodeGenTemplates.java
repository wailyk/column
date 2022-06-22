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
package org.apache.asterix.optimizer.rules.codegen.node;

import org.apache.asterix.optimizer.rules.codegen.node.control.WhileCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.LongLiteralCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.member.ArrayGetValueCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.member.MethodCallCodeNode;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class CodeGenTemplates {
    //Constants
    public static final LongLiteralCodeNode ZERO = new LongLiteralCodeNode(0);
    public static final LongLiteralCodeNode ONE = new LongLiteralCodeNode(1);

    //Variables' names
    public static final IdentifierCodeNode CURSOR = new IdentifierCodeNode("cursor");
    public static final IdentifierCodeNode RESULT_WRITER = new IdentifierCodeNode("resultWriter");
    public static final String READER_OBJECT = "reader";

    //Reader Members
    private static final IdentifierCodeNode IS_END_OF_ARRAY = new IdentifierCodeNode("isEndOfArray");
    private static final IdentifierCodeNode GET_VALUE = new IdentifierCodeNode("getValue");
    private static final IdentifierCodeNode TO_ARRAY = new IdentifierCodeNode("toArray");
    private static final IdentifierCodeNode REWIND = new IdentifierCodeNode("rewind");

    //Array Members
    private static final IdentifierCodeNode COUNT = new IdentifierCodeNode("count");

    //Common members
    private static final IdentifierCodeNode NEXT = new IdentifierCodeNode("next");

    private CodeGenTemplates() {
    }

    public static WhileCodeNode createTupleIteration(CodeNodeContext context, SourceLocation sourceLocation) {
        BlockCodeNode whileBlock = context.createAndEnterBlock(LogicalOperatorTag.DATASOURCESCAN);
        return new WhileCodeNode(sourceLocation, getMethod(CURSOR, NEXT), whileBlock);
    }
    /* ********************************
     * Reader
     * ********************************
     */

    public static IdentifierCodeNode createReaderObject(int index) {
        return new IdentifierCodeNode(READER_OBJECT + index);
    }

    public static ICodeNode getNext(IdentifierCodeNode object) {
        return getMethod(object, NEXT);
    }

    public static ICodeNode getIsEndOfArray(IdentifierCodeNode object) {
        return getMethod(object, IS_END_OF_ARRAY);
    }

    public static ICodeNode callGetValue(IdentifierCodeNode object) {
        return getMethod(object, GET_VALUE);
    }

    private static ICodeNode getMethod(IdentifierCodeNode object, IdentifierCodeNode methodName) {
        return new MethodCallCodeNode(object, methodName);
    }

    public static ICodeNode[] appendResultWriter(ICodeNode[] args) {
        ICodeNode[] newArgs = new ICodeNode[args.length + 1];
        System.arraycopy(args, 0, newArgs, 0, args.length);
        newArgs[args.length] = CodeGenTemplates.RESULT_WRITER;
        return newArgs;
    }

    public static ICodeNode createToArray(IdentifierCodeNode arrayReader) {
        return getMethod(arrayReader, TO_ARRAY);
    }

    public static ICodeNode createArrayCount(IdentifierCodeNode arrayVar) {
        return getMethod(arrayVar, COUNT);
    }

    public static ICodeNode createArrayGetValue(IdentifierCodeNode arrayVar, IdentifierCodeNode indexVar) {
        return new ArrayGetValueCodeNode(arrayVar, indexVar);
    }

    public static ICodeNode createRewind(IdentifierCodeNode reader) {
        return getMethod(reader, REWIND);
    }
}
