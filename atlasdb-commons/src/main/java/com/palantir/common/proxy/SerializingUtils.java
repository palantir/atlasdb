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
package com.palantir.common.proxy;

import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.ByteArrayIOStream;
import com.palantir.util.ObjectInputStreamFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@SuppressWarnings("BanSerializableRead")
public final class SerializingUtils {
    private static final SafeLogger log = SafeLoggerFactory.get(SerializingUtils.class);

    private SerializingUtils() {
        /* */
    }

    public static <T> T copy(T orig) {
        return copy(orig, (is, _codebase) -> new ObjectInputStream(is));
    }

    @SuppressWarnings("unchecked")
    public static <T> T copy(T orig, ObjectInputStreamFactory factory) {
        T obj = null;

        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            // Write the object out to a byte array
            ByteArrayIOStream byteStream = new ByteArrayIOStream();
            out = new ObjectOutputStream(byteStream);
            out.writeObject(orig);
            out.close();

            // Make an input stream from the byte array and read
            // a copy of the object back in.
            in = factory.create(byteStream.getInputStream(), null);
            obj = (T) in.readObject();
        } catch (IOException e) {
            log.error("IO exception", e);
        } catch (ClassNotFoundException cnfe) {
            log.error("class not found exception", cnfe);
        } finally {
            closeQuietly(in);
            closeQuietly(out);
        }
        return obj;
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            // Ignore
        }
    }
}
