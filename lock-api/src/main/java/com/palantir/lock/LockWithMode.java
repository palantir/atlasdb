package com.palantir.lock;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LockWithMode {
    private LockDescriptor lockDescriptor;
    private LockMode lockMode;

    public LockWithMode(@JsonProperty("lockDescriptor") LockDescriptor lockDescriptor,
                        @JsonProperty("lockMode") LockMode lockMode) {
        this.lockDescriptor = lockDescriptor;
        this.lockMode = lockMode;
    }

    public LockDescriptor getLockDescriptor() {
        return lockDescriptor;
    }

    public LockMode getLockMode() {
        return lockMode;
    }
}
