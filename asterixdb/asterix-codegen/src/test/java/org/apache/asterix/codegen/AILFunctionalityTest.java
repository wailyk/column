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
package org.apache.asterix.codegen;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import org.apache.asterix.codegen.asterix.column.reader.AbstractTypedColumnReader;
import org.apache.asterix.codegen.common.TestBase;
import org.apache.asterix.codegen.common.TestCase;
import org.apache.asterix.codegen.common.TruffleTestUtil;
import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.runtime.AILStringRuntime;
import org.apache.asterix.codegen.truffle.runtime.reader.column.AILColumnReader;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AILFunctionalityTest extends TestBase {
    private static final String ARG_ANNOTATION = "//@";

    public AILFunctionalityTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
    }

    @BeforeClass
    public static void setup() throws IOException {
        setup(AILFunctionalityTest.class);
    }

    @Parameters(name = "CodeTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return initTests(AILFunctionalityTest.class, "ail_functionality_test");
    }

    @Test
    public void runTest() throws IOException {
        File codeFile = testCase.getTestFile();
        execute(IOUtils.toString(codeFile.toURI(), StandardCharsets.UTF_8));
        Assert.assertTrue(testCase.compare());
    }

    private void execute(String code) throws IOException {
        CONTEXT.eval(AILLanguage.ID, code);
        Object[] args = getArgs(code);
        writeResult(testCase.getOutputFile(), CONTEXT.getBindings(AILLanguage.ID).getMember("test").execute(args));
    }

    private Object[] getArgs(String code) throws HyracksDataException {
        int indexOfArgsLine = code.indexOf(ARG_ANNOTATION);
        if (indexOfArgsLine < 0) {
            return new Object[0];
        }
        String argsLine =
                code.substring(indexOfArgsLine + ARG_ANNOTATION.length(), code.indexOf('\n', indexOfArgsLine));

        String[] argsCode = argsLine.split(",");
        Object[] args = new Object[argsCode.length];
        for (int i = 0; i < argsCode.length; i++) {
            String arg = argsCode[i].trim();

            char firstChar = arg.charAt(0);
            if (firstChar == '$') {
                args[i] = getString(arg);
            } else if (firstChar == 'r') {
                args[i] = getReader(arg);
            } else if (firstChar == 'n') {
                args[i] = getNumerical(arg);
            } else if (firstChar == 'b') {
                args[i] = getBoolean(arg);
            } else {
                throw new IllegalArgumentException("Invalid argument format '" + arg + "'");
            }
        }
        return args;
    }

    private static AILStringRuntime getString(String arg) throws HyracksDataException {
        return TruffleTestUtil.createTruffleString(arg.substring(1));
    }

    private static AILColumnReader getReader(String arg) {
        String[] typeLimitPair = arg.substring(1).split(":");
        ATypeTag typeTag;
        switch (typeLimitPair[0]) {
            case "bool":
                typeTag = ATypeTag.BOOLEAN;
                break;
            case "int":
                typeTag = ATypeTag.BIGINT;
                break;
            case "double":
                typeTag = ATypeTag.DOUBLE;
                break;
            case "string":
                typeTag = ATypeTag.STRING;
                break;
            default:
                throw new IllegalStateException("Unsupported type '" + typeLimitPair[0] + "'");
        }
        int limit = Integer.parseInt(typeLimitPair[1]);
        return new AILColumnReader(AbstractTypedColumnReader.createReader(typeTag));
    }

    private Object getNumerical(String arg) {
        String numericalString = arg.substring(1);
        if (numericalString.contains(".")) {
            return Double.parseDouble(numericalString);
        }
        return Long.parseLong(numericalString);
    }

    private Object getBoolean(String arg) {
        return Boolean.parseBoolean(arg.substring(1));
    }

}
