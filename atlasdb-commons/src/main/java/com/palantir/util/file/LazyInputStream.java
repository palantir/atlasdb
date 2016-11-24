/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.util.file;

import java.io.IOException;
import java.io.InputStream;

public class LazyInputStream extends InputStream {
    // TODO Construct this class with a function pointing to loadSingleBlockToOutputStream
    // This will be our buffer-loading method.
    // This class will contain a byte array (or maybe an OutputStream?) that is our buffer
    // Read() will fetch a byte from the buffer, and if the buffer is emptied, refresh the buffer
    // close() will close the output stream (if necessary).

    @Override
    public int read() throws IOException {
        return 0;
    }
}
