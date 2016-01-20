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

/**
 * This exception is thrown by the remote service when the client
 * partition map version is older than server's.
 *
 * It is then serialized over HTTP and is meant to be intercepted
 * by the local KeyValueEndpoint instance. That class should throw
 * a new instance of this class with the pmsUri filled in.
 *
 * The <code>PartitionedKeyValueService</code> can than catch it and
 * download new partition map using the provided URI.
 *
 * @see FillInUrlProxy
 * @see PartitionedKeyValueService
 *
 * @author htarasiuk
 *
 */
@Immutable public class ClientVersionTooOldException extends VersionMismatchException {

    private static final long serialVersionUID = 1L;

    private final String pmsUri;

    /**
     * Only use this if the pmsUri has been filled in.
     *
     * @return An updated version of the partion map; downloaded from the PartitionMapService
     * associated with the KeyValueService that threw the original exception.
     */
    public DynamicPartitionMap getUpdatedMap() {
    	return RemotingPartitionMapService.createClientSide(
    			Preconditions.checkNotNull(pmsUri)).getMap();
    }

    public ClientVersionTooOldException(String pmsUri) {
        // WARNING! The message has to be filled in with pmsUri since some
        // Palantir Throwables rewrapping logic will recreate the exception by
        // passing the getMessage() result to the single-argument constructor.
        super(pmsUri);
    	this.pmsUri = Preconditions.checkNotNull(pmsUri);
    }

    public ClientVersionTooOldException() {
        super(null);
    	this.pmsUri = null;
    }

	@Override
	public String toString() {
		return "ClientVersionTooOldException [pmsUri=" + pmsUri + "]";
	}
}
