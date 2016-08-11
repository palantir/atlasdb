/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.remoting;

import java.util.Objects;

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.exception.EndpointVersionTooOldException;

import feign.Response;
import feign.codec.ErrorDecoder;

public final class KeyValueServiceErrorDecoder implements ErrorDecoder {
    private static final ErrorDecoder defaultDecoder = new ErrorDecoder.Default();
    private static final KeyValueServiceErrorDecoder instance = new KeyValueServiceErrorDecoder();

    private KeyValueServiceErrorDecoder() {
        // singleton
    }

    public static KeyValueServiceErrorDecoder instance() {
        return instance;
    }

    @Override
    public Exception decode(String methodKey, Response response) {
        if (response != null) {
            if (response.status() == 409) {
                return new KeyAlreadyExistsException(Objects.toString(response.body()));
            }
            if (response.status() == 503) {
                return new InsufficientConsistencyException(Objects.toString(response.body()));
            }
            if (response.status() == 410) {
                return new ClientVersionTooOldException();
            }
            if (response.status() == 406) {
                return new EndpointVersionTooOldException();
            }
        }
        return defaultDecoder.decode(methodKey, response);
    }
}
