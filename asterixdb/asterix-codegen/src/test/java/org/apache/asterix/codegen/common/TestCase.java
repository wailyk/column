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
package org.apache.asterix.codegen.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestCase {
    private static final Logger LOGGER = LogManager.getLogger();
    private final File testFile;
    private final File resultFile;
    private final File outputFile;

    public TestCase(File testFile, File resultFile, File outputPath) {
        this.testFile = testFile;
        this.resultFile = resultFile;
        this.outputFile = new File(outputPath, resultFile.getName());
    }

    public File getTestFile() {
        return testFile;
    }

    public File getOutputFile() {
        return outputFile;
    }

    public boolean compare() throws IOException {
        try (BufferedReader result = new BufferedReader(new FileReader(resultFile));
                BufferedReader output = new BufferedReader(new FileReader(outputFile))) {
            int line = 1;
            String outLine = output.readLine();
            while (outLine != null) {
                String resultLine = result.readLine();
                if (!outLine.equals(resultLine)) {
                    LOGGER.fatal("Unexpected line [{}]\n Expected:{} \n Actual: {}", line, resultLine, outLine);
                    return false;
                }
                outLine = output.readLine();
                line++;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return testFile.getName();
    }
}