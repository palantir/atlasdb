/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.atlasdb.http.v2;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.UnsafeArg;

/**
 * Represents a value which is computed by lazily applying the given mapping function to
 * the result of an 'input' supplier. If the input hasn't changed, we avoid re-running the mapping function (as this
 * may be expensive).
 *
 * <p>This is effectively just Witchcraft's {@code Refreshable} class.
 */
final class CachedComposingSupplier<T, U> implements Supplier<U> {
    private static final Logger log = LoggerFactory.getLogger(CachedComposingSupplier.class);
    private final Supplier<T> inputSupplier;
    private final Function<T, U> outputFunction;

    private T cachedInput = null;
    private U cachedOutput = null;

    CachedComposingSupplier(Supplier<T> inputSupplier, Function<T, U> outputFunction) {
        this.inputSupplier = inputSupplier;
        this.outputFunction = outputFunction;
    }

    public <V> CachedComposingSupplier<U, V> map(Function<U, V> map) {
        return new CachedComposingSupplier<>(this, map);
    }

    @Override
    public synchronized U get() {
        T currentInput = inputSupplier.get();

        if (Objects.equals(cachedInput, currentInput)) {
            return cachedOutput;
        }

        cachedInput = currentInput;
        U newOutput = outputFunction.apply(cachedInput);
        if (!Objects.equals(newOutput, cachedOutput) && cachedOutput instanceof Closeable) {
            try {
                ((Closeable) cachedOutput).close();
            } catch (IOException e) {
                log.warn("Failed to close Closeable", UnsafeArg.of("cachedOutput", cachedOutput), e);
            }
        }
        cachedOutput = newOutput;
        return cachedOutput;
    }
}
