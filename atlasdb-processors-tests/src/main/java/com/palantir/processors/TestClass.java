/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.processors;

@AutoDelegate(typeToExtend = TestClass.class)
public class TestClass {
    private static void privateStaticMethod() {}
    protected static int protectedStaticMethod() {
        return 0;
    }
    public static Boolean publicStaticMethod() {
        return Boolean.TRUE;
    }

    private TestClass(boolean privateConstructor) {}

    protected TestClass(String protectedConstructor) {}

    public TestClass(int publicConstructor) {}

    public TestClass() {}

    public void methodWithNoParameters() {}
    public void methodWithOneParameter(int integer) {}

    protected void protectedMethod() {}
    private void privateMethod() {}
}
