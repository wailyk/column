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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.codegen.asterix.CodeGenerationProjectionInfo;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.codegen.ScalarExpressionCodeGenVisitor;
import org.apache.asterix.optimizer.rules.codegen.node.control.WhileCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.AppendBuiltinFunctionCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.builtin.FlushBuiltinFunctionCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.expression.literal.IdentifierCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.visitor.StringCodeGeneratorVisitor;
import org.apache.asterix.optimizer.rules.codegen.schema.CodeGenExpectedSchemaBuilder;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CodeNodeContext {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Set<LogicalVariable> originalVariables;
    private final Map<String, FunctionCallInformation> functionCallInformationMap;
    private final DataSourceScanOperator scanOp;
    private final CodeGenExpectedSchemaBuilder schemaBuilder;
    private final Map<LogicalVariable, ICodeNode> outputExpr;
    private final List<IExpectedSchemaNode> readersSchemas;
    private final Map<LogicalVariable, IdentifierCodeNode> sourceVariableToLanguageVariable;
    private final Map<IExpectedSchemaNode, IdentifierCodeNode> schemaToReaderArg;
    private final Map<AnyExpectedSchemaNode, IAType> paths;
    private final Map<LogicalVariable, ICodeNode> nestedExpressionsToVariable;
    private final Map<IdentifierCodeNode, IdentifierCodeNode> arrayVarToIndexVar;
    //Maps each expression to its set of used variables
    private final Map<LogicalVariable, Set<IdentifierCodeNode>> exprToUsedReader;
    private final MainFunctionCodeNode main;
    private int nestedScope;
    private BlockCodeNode currentBlock;
    private int varCount;

    public CodeNodeContext(String functionName, DataSourceScanOperator scanOp,
            CodeGenExpectedSchemaBuilder schemaBuilder, Map<AnyExpectedSchemaNode, IAType> paths,
            Map<String, FunctionCallInformation> functionCallInformationMap) {
        this.scanOp = scanOp;
        this.schemaBuilder = schemaBuilder;
        this.paths = new HashMap<>(paths);
        originalVariables = new HashSet<>(scanOp.getVariables());
        this.functionCallInformationMap = functionCallInformationMap;
        sourceVariableToLanguageVariable = new HashMap<>();
        outputExpr = new HashMap<>();
        readersSchemas = new ArrayList<>();
        schemaToReaderArg = new HashMap<>();
        nestedExpressionsToVariable = new HashMap<>();
        arrayVarToIndexVar = new HashMap<>();
        exprToUsedReader = new HashMap<>();
        varCount = 0;
        nestedScope = 0;
        main = createMainFunction(functionName);
    }

    private MainFunctionCodeNode createMainFunction(String functionName) {
        BlockCodeNode mainBlock = createAndEnterBlock(LogicalOperatorTag.EMPTYTUPLESOURCE);
        SourceLocation sourceLocation = scanOp.getSourceLocation();
        MainFunctionCodeNode newMain =
                new MainFunctionCodeNode(functionName, scanOp.getSourceLocation(), mainBlock, paths.size());
        WhileCodeNode whileNode = CodeGenTemplates.createTupleIteration(this, sourceLocation);
        mainBlock.appendNode(whileNode);
        return newMain;
    }

    public IdentifierCodeNode createNewVariable() {
        return new IdentifierCodeNode("var" + varCount++);
    }

    public MainFunctionCodeNode getMain() {
        return main;
    }

    public void finish(ScalarExpressionCodeGenVisitor expressionVisitor, StringCodeGeneratorVisitor codeGenerator)
            throws AlgebricksException {
        List<LogicalVariable> variables = scanOp.getProjectVariables();
        Set<ICodeNode> alreadyOutput = new HashSet<>();
        expressionVisitor.enterScope(this);
        for (LogicalVariable variable : variables) {
            ICodeNode output;
            if (outputExpr.containsKey(variable)) {
                output = outputExpr.get(variable);
            } else {
                output = expressionVisitor.toNode(variable);
            }
            if (!alreadyOutput.contains(output)) {
                currentBlock.appendNode(new AppendBuiltinFunctionCodeNode(output));
                alreadyOutput.add(output);
            }

        }
        currentBlock.appendNode(new FlushBuiltinFunctionCodeNode());

        main.setPaths(getFinalReaderSchemas());
        String code = codeGenerator.generateCode(main);
        scanOp.setDatasetProjectionInfo(new CodeGenerationProjectionInfo(main.getFunctionName().toString(), code,
                main.getPaths(), functionCallInformationMap));
        LOGGER.info(code);
    }

    public BlockCodeNode getCurrentBlock() {
        return currentBlock;
    }

    public void enterBlock(BlockCodeNode blockNode) {
        currentBlock = blockNode;
    }

    public BlockCodeNode createBlock(LogicalOperatorTag tag) {
        return new BlockCodeNode(currentBlock, tag);
    }

    public BlockCodeNode createAndEnterBlock(LogicalOperatorTag tag) {
        currentBlock = createBlock(tag);
        return currentBlock;
    }

    public ICodeNode getValue(IExpectedSchemaNode schemaNode, Set<IdentifierCodeNode> usedReaders) {
        if (!isReaderBounded(schemaNode)) {
            //We may need to call next here as it is an inlined expression (e.g., select($x.getValue("age") >= 21))
            currentBlock.appendNode(createNext(schemaNode, null));
        }

        IdentifierCodeNode reader = schemaToReaderArg.get(schemaNode);
        usedReaders.add(reader);
        return CodeGenTemplates.callGetValue(reader);
    }

    public ICodeNode getValue(LogicalVariable sourceVariable, Set<IdentifierCodeNode> usedReaders) {
        if (nestedExpressionsToVariable.containsKey(sourceVariable)) {
            return nestedExpressionsToVariable.get(sourceVariable);
        }
        IdentifierCodeNode reader = schemaToReaderArg.get(schemaBuilder.getNodeFromVariable(sourceVariable));
        usedReaders.add(reader);
        if (currentBlock.isReaderOverridden(reader)) {
            IdentifierCodeNode arrayVar = currentBlock.getOverriddenReaderVariable(reader);
            LogicalVariable posVar = schemaBuilder.getPositionalVariableIfAny(sourceVariable);

            IdentifierCodeNode indexVar;
            if (posVar != null) {
                indexVar = (IdentifierCodeNode) nestedExpressionsToVariable.get(posVar);
            } else {
                indexVar = arrayVarToIndexVar.get(arrayVar);
            }
            return CodeGenTemplates.createArrayGetValue(arrayVar, indexVar);
        }

        return sourceVariableToLanguageVariable.computeIfAbsent(sourceVariable, k -> {
            exprToUsedReader.put(k, new HashSet<>(usedReaders));
            return currentBlock.declareVariable(this, CodeGenTemplates.callGetValue(reader));
        });

    }

    public boolean isReaderBounded(IExpectedSchemaNode schemaNode) {
        return schemaToReaderArg.containsKey(schemaNode);
    }

    public ICodeNode createNext(IExpectedSchemaNode schemaNode, BlockCodeNode loopBlock) {
        IdentifierCodeNode reader = bindReader(schemaNode);
        if (currentBlock.isReaderOverridden(reader)) {
            reader = currentBlock.getOverriddenReaderVariable(reader);
        }
        currentBlock.bindReader(reader);
        if (loopBlock != null) {
            loopBlock.bindReader(reader);
        }
        return CodeGenTemplates.getNext(reader);
    }

    public ICodeNode createToArray(IExpectedSchemaNode schemaNode, IdentifierCodeNode indexVar,
            BlockCodeNode loopBlock) {
        IdentifierCodeNode reader = bindReader(schemaNode);
        IdentifierCodeNode arrayVar;
        if (currentBlock.isReaderOverridden(reader)) {
            arrayVar = currentBlock.getOverriddenReaderVariable(reader);
        } else {
            arrayVar = currentBlock.declareVariable(this, CodeGenTemplates.createToArray(reader));
            currentBlock.bindReader(reader);
        }
        loopBlock.bindReader(reader, arrayVar);
        arrayVarToIndexVar.put(arrayVar, indexVar);
        return arrayVar;
    }

    private IdentifierCodeNode bindReader(IExpectedSchemaNode schemaNode) {
        return schemaToReaderArg.computeIfAbsent(schemaNode, k -> {
            IdentifierCodeNode availableReader = main.getReaderArgs().get(readersSchemas.size());
            readersSchemas.add(k);
            return availableReader;
        });
    }

    public int getNumberOfUsedReaders() {
        return readersSchemas.size();
    }

    public void addToScan(LogicalVariable variable, Object type) {
        scanOp.projectVariable(variable, type);
    }

    public void putOutput(LogicalVariable variable, ICodeNode output, Object type) {
        if (isNestedScope()) {
            return;
        }
        outputExpr.put(variable, output);
        scanOp.projectVariable(variable, type);
        nestedExpressionsToVariable.put(variable, output);
    }

    public void pushAssignToDataScan(LogicalVariable variable, Object type) {
        if (isNestedScope()) {
            return;
        }
        scanOp.projectVariable(variable, type);
    }

    public void projectOutput(List<LogicalVariable> projectedVars) {
        if (isNestedScope()) {
            return;
        }
        for (LogicalVariable variable : projectedVars) {
            if (sourceVariableToLanguageVariable.containsKey(variable)) {
                outputExpr.computeIfAbsent(variable, sourceVariableToLanguageVariable::get);
            }
        }
        outputExpr.keySet().retainAll(projectedVars);
        scanOp.projectOnly(projectedVars);
    }

    public void clearOutput() {
        outputExpr.clear();
        scanOp.clearProject();
    }

    public void exitToMainBlock() {
        currentBlock = main.getBlock();
    }

    public boolean shouldProject(List<LogicalVariable> variables) {
        for (LogicalVariable variable : variables) {
            if (originalVariables.contains(variable)) {
                return false;
            }
        }
        return true;
    }

    public IdentifierCodeNode getReader(IExpectedSchemaNode node) {
        for (int i = 0; i < readersSchemas.size(); i++) {
            if (readersSchemas.get(i) == node) {
                return main.getReader(i);
            }
        }
        throw new IllegalStateException("Reader for the schema " + node + " is not bound");
    }

    private ARecordType[] getFinalReaderSchemas() {
        ARecordType[] finalSchemas = new ARecordType[readersSchemas.size()];
        for (int i = 0; i < readersSchemas.size(); i++) {
            AnyExpectedSchemaNode schemaNode = (AnyExpectedSchemaNode) readersSchemas.get(i);
            finalSchemas[i] = (ARecordType) paths.get(schemaNode);
        }
        return finalSchemas;
    }

    public void assign(LogicalVariable variable, ICodeNode nestedExpression, Set<IdentifierCodeNode> usedReaders) {
        IdentifierCodeNode exprVar;
        if (nestedExpression.getType() != CodeNodeType.IDENTIFIER) {
            exprVar = currentBlock.declareVariable(this, nestedExpression);
            exprToUsedReader.put(variable, new HashSet<>(usedReaders));
        } else {
            exprVar = (IdentifierCodeNode) nestedExpression;
        }
        nestedExpressionsToVariable.put(variable, exprVar);
    }

    public boolean isReaderBoundedToBlock(LogicalVariable variable, BlockCodeNode block) {
        if (exprToUsedReader.containsKey(variable)) {
            for (IdentifierCodeNode usedReader : exprToUsedReader.get(variable)) {
                if (block.isReaderBounded(usedReader)) {
                    return true;
                }
            }
        }
        IdentifierCodeNode reader = schemaToReaderArg.get(schemaBuilder.getNodeFromVariable(variable));
        return block.isReaderBounded(reader);
    }

    public Map<LogicalVariable, IdentifierCodeNode> enterNestedScope() {
        nestedScope++;
        return new HashMap<>(sourceVariableToLanguageVariable);
    }

    public void exitNestedScope(Map<LogicalVariable, IdentifierCodeNode> scope) {
        nestedScope--;
        sourceVariableToLanguageVariable.clear();
        sourceVariableToLanguageVariable.putAll(scope);
    }

    public boolean isNestedScope() {
        return nestedScope > 0;
    }

    public void bindPositionalVariable(LogicalVariable unnestVariable, LogicalVariable positionalVariable) {
        schemaBuilder.setPositionalVariable(unnestVariable, positionalVariable);
    }
}
