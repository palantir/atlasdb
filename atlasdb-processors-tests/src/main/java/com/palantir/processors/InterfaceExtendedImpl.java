/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

@AutoDelegate(interfaceToExtend = InterfaceExtended.class)
public class InterfaceExtendedImpl implements AutoDelegate_InterfaceExtended {
    @Override
    public InterfaceExtended delegate() {
        return null;
    }

    @Override
    public int methodWithReturnTypeAndParameters(int p1) {
        return 0;
    }

    @Override
    public void overloadedMethod(Integer p1, Integer p2) {

    }

    @Override
    public void overloadedMethod(int p1) {

    }

    @Override
    public void methodWithOneParameter(int p1) {

    }

    @Override
    public int methodWithReturnTypeAndVarArgs(int... parameters) {
        return 0;
    }

    @Override
    public void methodWithTwoParameters(int p1) {

    }

    @Override
    public int methodWithReturnType() {
        return 0;
    }

    @Override
    public void defaultMethod() {

    }

    @Override
    public void methodWithNoParameters() {

    }

    @Override
    public void methodWithVarArgs(int... parameters) {

    }

    @Override
    public void overridenMethod(Integer p1, Integer p2, Integer p3) {

    }

    @Override
    public void overloadedMethod() {

    }
}
