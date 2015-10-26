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
package com.palantir.atlasdb.keyvalue.partition.exception;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.common.annotation.Immutable;

@Immutable public class EndpointVersionTooOldException extends VersionMismatchException {

    private static final long serialVersionUID = 6421197986192185450L;
    private final String pmsUri;

    /**
     * Only use if pmsUri has been filled in.
     *
     * @param map
     */
    public void pushNewMap(DynamicPartitionMap map) {
        RemotingPartitionMapService.createClientSide(
                Preconditions.checkNotNull(pmsUri)).updateMapIfNewer(map);
    }

    public EndpointVersionTooOldException() {
        super(null);
        this.pmsUri = null;
    }

    public EndpointVersionTooOldException(String pmsUri) {
        super(pmsUri);
        this.pmsUri = pmsUri;
    }

    public EndpointVersionTooOldException(String message, Throwable cause) {
        super(message, cause);
        this.pmsUri = message;
    }

    @Override
    public String toString() {
        return "EndpointVersionTooOldException [pmsUri=" + pmsUri + "]";
    }

}
