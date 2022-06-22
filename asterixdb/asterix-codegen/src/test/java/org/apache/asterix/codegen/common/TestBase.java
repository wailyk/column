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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.IoUtil;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.Value;

public abstract class TestBase {
    protected static final Context CONTEXT;
    protected static final File OUTPUT_PATH;

    protected static final String SOURCE_PATH;
    private static final String ONLY_TESTS;
    private static final String RESULT_PATH;
    private static final Map<Class<?>, TestPath> TEST_PATH_MAP;

    protected final TestCase testCase;

    static {
        CONTEXT = Context.newBuilder(AILLanguage.ID).allowPolyglotAccess(PolyglotAccess.ALL).allowAllAccess(true)
                .allowHostAccess(HostAccess.ALL).allowExperimentalOptions(true).build();
        TEST_PATH_MAP = new HashMap<>();

        ClassLoader classLoader = TestBase.class.getClassLoader();
        OUTPUT_PATH = new File("target", "result");
        ONLY_TESTS = "only.txt";
        SOURCE_PATH = "code";
        RESULT_PATH = "result";
    }

    protected TestBase(TestCase testCase) throws HyracksDataException {
        this.testCase = testCase;
    }

    protected static void setup(Class<?> clazz) throws IOException {
        TestPath path = TEST_PATH_MAP.get(clazz);
        if (!OUTPUT_PATH.exists()) {
            Files.createDirectory(Paths.get(OUTPUT_PATH.toURI()));
        }
        if (path.outputPath.exists()) {
            IoUtil.delete(path.outputPath);
        }
        Files.createDirectory(Paths.get(path.outputPath.toURI()));
    }

    protected static Collection<Object[]> initTests(Class<?> clazz, String testDir) throws Exception {
        TestPath path = TEST_PATH_MAP.computeIfAbsent(clazz, k -> new TestPath(testDir));
        Set<String> only = getOnly(path.onlyPath);
        List<File> testFiles = listFiles(path.sourcePath, only);
        List<File> resultFiles = listFiles(path.resultPath, only);

        List<Object[]> testCases = new ArrayList<>();
        for (int i = 0; i < testFiles.size(); i++) {
            Object[] testCase = { new TestCase(testFiles.get(i), resultFiles.get(i), path.outputPath) };
            testCases.add(testCase);
        }
        return testCases;
    }

    protected static void writeResult(File resultFile, Value result) throws IOException {
        try (PrintStream ps = new PrintStream(new FileOutputStream(resultFile))) {
            ps.println(result);
        }
    }

    private static List<File> listFiles(File path, Set<String> only) throws IOException {
        Predicate<File> predicate = f -> only.isEmpty() || only.contains(f.getName().split("\\.")[0]);
        return Files.list(Paths.get(path.toURI())).map(Path::toFile).filter(predicate).sorted(File::compareTo)
                .collect(Collectors.toList());
    }

    private static Set<String> getOnly(File onlyPath) throws FileNotFoundException {
        BufferedReader reader = new BufferedReader(new FileReader(onlyPath));
        return reader.lines().filter(l -> !l.isEmpty() && l.charAt(0) != '#').collect(Collectors.toSet());
    }

    private static File getResourceFile(String testDir, String sourcePath) {
        ClassLoader classLoader = TestBase.class.getClassLoader();
        return new File(
                Objects.requireNonNull(classLoader.getResource(testDir + File.separator + sourcePath)).getPath());
    }

    private static class TestPath {
        private final File onlyPath;
        private final File sourcePath;
        private final File resultPath;

        private final File outputPath;

        TestPath(String testDir) {
            this.onlyPath = getResourceFile(testDir, ONLY_TESTS);
            this.sourcePath = getResourceFile(testDir, SOURCE_PATH);
            this.resultPath = getResourceFile(testDir, RESULT_PATH);
            this.outputPath = new File(OUTPUT_PATH, testDir);
        }
    }
}
