package com.palantir.atlasdb.keyvalue.impl;


public abstract class CloseShieldedKeyValueService extends ForwardingKeyValueService {
    @Override
    public void close() {
        // We block close from going through.
    }
}
