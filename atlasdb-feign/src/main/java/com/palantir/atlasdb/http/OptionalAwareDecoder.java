/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Optional;

import feign.FeignException;
import feign.Response;
import feign.codec.Decoder;

public class OptionalAwareDecoder implements Decoder {
    private final Decoder delegate;

    public OptionalAwareDecoder(Decoder delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object decode(Response response, Type type) throws IOException, FeignException {
        if (response.status() == 204 && isOptional(type)) {
            return Optional.empty();
        }
        return delegate.decode(response, type);
    }

    private boolean isOptional(Type type) {
        return type instanceof ParameterizedType
                && ((ParameterizedType) type).getRawType().equals(Optional.class);
    }
}
