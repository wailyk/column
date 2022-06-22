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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class MainFunctionCodeNode extends AbstractCodeNode {
    private static final long serialVersionUID = -2528753899995266854L;
    private static final String FUNCTION_NAME_POSTFIX = "Func";

    private final List<IdentifierCodeNode> args;
    private final List<IdentifierCodeNode> readerArgs;
    private final BlockCodeNode block;
    private final ARecordType[] paths;
    private final IdentifierCodeNode functionName;

    public MainFunctionCodeNode(String functionName, SourceLocation sourceLocation, BlockCodeNode block,
            int numberOfReaders) {
        super(sourceLocation);
        this.functionName = new IdentifierCodeNode(functionName + FUNCTION_NAME_POSTFIX);
        args = new ArrayList<>();
        readerArgs = new ArrayList<>();
        this.block = block;
        this.paths = new ARecordType[numberOfReaders];

        //Add cursor as a parameter
        args.add(CodeGenTemplates.CURSOR);
        args.add(CodeGenTemplates.RESULT_WRITER);
        for (int i = 0; i < numberOfReaders; i++) {
            IdentifierCodeNode reader = CodeGenTemplates.createReaderObject(i);
            args.add(reader);
            readerArgs.add(reader);
        }
    }

    public BlockCodeNode getBlock() {
        return block;
    }

    public List<IdentifierCodeNode> getArgs() {
        return args;
    }

    public List<IdentifierCodeNode> getReaderArgs() {
        return readerArgs;
    }

    public IdentifierCodeNode getFunctionName() {
        return functionName;
    }

    @Override
    public <R, T> R accept(ICodeNodeVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visit(this, arg);
    }

    public IdentifierCodeNode getReader(int index) {
        return readerArgs.get(index);
    }

    public void setPaths(ARecordType[] paths) {
        System.arraycopy(paths, 0, this.paths, 0, paths.length);
    }

    public ARecordType[] getPaths() {
        return paths;
    }

    @Override
    public CodeNodeType getType() {
        return CodeNodeType.FUNCTION;
    }
}
