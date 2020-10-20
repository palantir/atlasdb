/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.util;

import com.github.peterwippermann.junit4.parameterizedsuite.ParameterContext;
import com.google.common.collect.ImmutableSet;
import java.util.function.Supplier;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class ParameterInjector<T extends TestRule> implements TestRule {

    private final Supplier<T> factory;
    private boolean outsideSuite;
    private T currentInstance = null;

    private ParameterInjector(Supplier<T> factory) {
        this.factory = factory;
    }

    public static <T extends TestRule> ParameterInjector<T> withFallBackConfiguration(Supplier<T> factory) {
        return new ParameterInjector<>(factory);
    }

    public Iterable<T> getParameter() {
        if (ParameterContext.isParameterSet()) {
            return ImmutableSet.of(ParameterContext.getParameter());
        } else {
            outsideSuite = true;
            currentInstance = factory.get();
            return ImmutableSet.of(currentInstance);
        }
    }

    @Override
    public Statement apply(Statement base, Description description) {
        if (outsideSuite) {
            return currentInstance.apply(base, description);
        } else {
            return base;
        }
    }
}
