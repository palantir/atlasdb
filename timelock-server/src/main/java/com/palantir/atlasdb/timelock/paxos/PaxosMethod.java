/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.paxos;

/**
 *
 */
public enum PaxosMethod {
    PREPARE,
    ACCEPT,
    GET_LATEST_PREPARED_OR_ACCEPTED,
    LEARN,
    GET_LEARNED_VALUE,
    GET_GREATEST_LEARNED_VALUE,
    GET_LEADED_VALUES_SINCE;
}
