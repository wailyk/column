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
package org.apache.asterix.codegen.call;

import java.util.Random;

import org.apache.asterix.codegen.truffle.AILLanguage;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.Value;
import org.junit.Assert;
import org.junit.Test;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;

public class MemberMethodCallTest {
    private static final Context CONTEXT = Context.newBuilder(AILLanguage.ID).allowPolyglotAccess(PolyglotAccess.ALL)
            .allowAllAccess(true).allowHostAccess(HostAccess.ALL).allowExperimentalOptions(true).build();

    @Test
    public void testJavaCall() {
        String code = "function test1(meInstance) { return meInstance.getMeLong(); }";
        CONTEXT.eval(AILLanguage.ID, code);
        Value meCallable = CONTEXT.getBindings(AILLanguage.ID).getMember("test1");
        Assert.assertEquals(10, meCallable.execute(new MeCallableClass(null)).asLong());
    }

    @Test
    public void testRandom() {
        String code = "function x(rand, end) {\n" + "\ti = 0;\n" + "\twhile(rand.getMeNextLong() != end) {\n"
                + "\t\ti = i + 1;\n" + "\t}\n" + "\treturn i;\n" + "}";
        CONTEXT.eval(AILLanguage.ID, code);
        Value meCallable = CONTEXT.getBindings(AILLanguage.ID).getMember("x");
        System.err.println(meCallable.execute(new MeCallableClass(null), Long.MAX_VALUE).asLong());
    }

    @Test
    public void testArray() {
        String code = "function x(rand, end) {\n" + "\ti = 0;\n" + "\twhile(rand[i] != end) {\n" + "\t\ti = i + 1;\n"
                + "\t}\n" + "\treturn i;\n" + "}";
        CONTEXT.eval(AILLanguage.ID, code);
        Value meCallable = CONTEXT.getBindings(AILLanguage.ID).getMember("x");
        System.err.println(meCallable.execute(new MeCallableClass(null).values, Long.MAX_VALUE).asLong());
    }

    @Test
    public void testRandom2() {
        String code = "function meFunction(x,y) {\n" + "\ti = 0;\n" + "\twhile(x.hasNext()) {\n"
                + "\t\ti = i ++ y.getValue();\n" + "\t}\n" + "\treturn i;\n" + "}";
        CONTEXT.eval(AILLanguage.ID, code);
        Value meCallable = CONTEXT.getBindings(AILLanguage.ID).getMember("meFunction");
        MeAnotherCallableClass anotherCallable = new MeAnotherCallableClass();
        MeCallableClass callableClass = new MeCallableClass(anotherCallable);
        System.err.println(meCallable.execute(callableClass, anotherCallable).asLong());
    }

    public static class MeCallableClass {
        private long[] values;
        private int index = 0;
        private final MeAnotherCallableClass anotherCallableClass;

        MeCallableClass(MeAnotherCallableClass anotherCallableClass) {
            Random random = new Random(0);
            int size = 1000000;
            values = new long[size];
            for (int i = 0; i < size - 1; i++) {
                values[i] = random.nextInt(1000000);
            }
            values[size - 1] = Long.MAX_VALUE;
            this.anotherCallableClass = anotherCallableClass;
        }

        @TruffleBoundary
        public long getMeNextLong() {
            return values[index++];
        }

        @TruffleBoundary
        public boolean hasNext() {
            boolean hasNext = index < values.length;
            if (hasNext) {
                anotherCallableClass.value = this;
            }
            return hasNext;
        }
    }

    public static class MeAnotherCallableClass {
        private MeCallableClass value;

        @TruffleBoundary
        public long getValue() {
            return value.getMeNextLong();
        }
    }

}
