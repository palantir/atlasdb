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

public interface ExpectedRandomInterface extends RandomInterface {
    RandomInterface delegate();

    @Override
    default void method1(int p1) {
        delegate().method1(p1);
    }

    @Override
    default void method1() {
        delegate().method1();
    }

    @Override
    default int method4(int p1, int p2) {
        return delegate().method4(p1,p2);
    }

    @Override
    default void method5() {
        delegate().method5();
    }

    @Override
    default void method2() {
        delegate().method2();
    }

    @Override
    default void method3(int p1) {
        delegate().method3(p1);
    }

    @Override
    default int method4(int p1) {
        return delegate().method4(p1);
    }
}
