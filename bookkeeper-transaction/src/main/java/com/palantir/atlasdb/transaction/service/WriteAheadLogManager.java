package com.palantir.atlasdb.transaction.service;

public interface WriteAheadLogManager {
    WriteAheadLog create();

    // The retrieved log is always closed
    WriteAheadLog retrieve(long logId);
}
