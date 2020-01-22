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

package com.palantir.atlasdb.http;

import com.palantir.conjure.java.api.errors.QosException;
import java.util.function.Function;
import javax.ws.rs.core.Response;
import org.immutables.value.Value;

interface AtlasDbHttpProtocolHandler<E extends Exception> {
    Response handleLegacyOrUnknownVersion(E underlyingException);

    QosException handleConjureJavaRuntime(E underlyingException);

    @Value.Immutable
    interface LambdaHandler<T extends Exception> extends AtlasDbHttpProtocolHandler<T> {
        @Value.Parameter
        Function<T, Response> legacyHandler();
        @Value.Parameter
        Function<T, QosException> conjureJavaRuntimeHandler();

        @Override
        default Response handleLegacyOrUnknownVersion(T underlyingException) {
            return legacyHandler().apply(underlyingException);
        }

        @Override
        default QosException handleConjureJavaRuntime(T underlyingException) {
            return conjureJavaRuntimeHandler().apply(underlyingException);
        }

        static <T extends Exception> LambdaHandler<T> of(
                Function<T, Response> legacyHandler, Function<T, QosException> conjureJavaRuntimeHandler) {
            return ImmutableLambdaHandler.of(legacyHandler, conjureJavaRuntimeHandler);
        }
    }
}
