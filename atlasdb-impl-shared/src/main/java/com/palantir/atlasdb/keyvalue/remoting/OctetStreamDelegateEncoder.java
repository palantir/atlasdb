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

import java.lang.reflect.Type;

import feign.RequestTemplate;
import feign.codec.EncodeException;
import feign.codec.Encoder;

public class OctetStreamDelegateEncoder implements Encoder {

    final Encoder delegate;

    public OctetStreamDelegateEncoder(Encoder delegate) {
        this.delegate = delegate;
    }

    @Override
    public void encode(Object object, Type bodyType, RequestTemplate template)
            throws EncodeException {
        if (object instanceof byte[]) {
            template.body((byte[]) object, null);
        } else {
            delegate.encode(object, bodyType, template);
        }
    }

}
