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
package com.palantir.lock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.logsafe.Preconditions;
import java.io.Serializable;
import java.math.BigInteger;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class LockRefreshToken implements Serializable {
    private static final long serialVersionUID = 1L;

    private final BigInteger tokenId;
    private final long expirationDateMs;

    @JsonCreator
    public LockRefreshToken(@JsonProperty("tokenId") BigInteger tokenId,
                            @JsonProperty("expirationDateMs") long expirationDateMs) {
        this.tokenId = Preconditions.checkNotNull(tokenId, "tokenId should not be null");
        this.expirationDateMs = expirationDateMs;
    }

    public BigInteger getTokenId() {
        return tokenId;
    }

    public long getExpirationDateMs() {
        return expirationDateMs;
    }

    public HeldLocksToken refreshTokenWithExpriationDate(HeldLocksToken token) {
        Preconditions.checkArgument(token.getTokenId().equals(tokenId), "token ids must match");
        return token.refresh(expirationDateMs);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tokenId == null) ? 0 : tokenId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LockRefreshToken other = (LockRefreshToken) obj;
        if (tokenId == null) {
            if (other.tokenId != null) {
                return false;
            }
        } else if (!tokenId.equals(other.tokenId)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "LockRefreshToken [tokenId=" + tokenId.toString(Character.MAX_RADIX) + "]";
    }
}
