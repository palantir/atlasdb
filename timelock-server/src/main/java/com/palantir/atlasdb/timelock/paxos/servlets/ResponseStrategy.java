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

package com.palantir.atlasdb.timelock.paxos.servlets;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

public interface ResponseStrategy<V> {
    /**
     * Returns the media type this ResponseStrategy is designed to handle.
     * @return A String representation of the media type this strategy handles
     */
    String mediaType();

    /**
     * Writes the provided valueToWrite to the HTTP response response, in a suitable format for the mediaType.
     * @param valueToWrite the value to include as the payload of the response
     * @param response the response to write the value to
     * @throws IOException if there was an I/O error writing to the response
     */
    void writeResponse(V valueToWrite, HttpServletResponse response) throws IOException;
}
