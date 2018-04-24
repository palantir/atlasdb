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

@AutoDelegate(typeToExtend = TestInterface.class)
public interface TestInterface {
    void methodWithNoParameters();
    void methodWithOneParameter(int p1);
    void methodWithTwoParameters(int p1, int p2);

    void methodWithVarArgs(int... parameters);

    int methodWithReturnType();
    int methodWithReturnTypeAndParameters(int p1);
    int methodWithReturnTypeAndVarArgs(int... parameters);

    void overloadedMethod();
    void overloadedMethod(int p1);
    void overloadedMethod(Integer p1, Integer p2);

    default void defaultMethod() {}

    void overriddenMethod(Integer p1, Integer p2, Integer p3);
}
