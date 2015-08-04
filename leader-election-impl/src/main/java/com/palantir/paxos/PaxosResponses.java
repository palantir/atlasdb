package com.palantir.paxos;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;

public class PaxosResponses {
    public static Predicate<PaxosResponse> isSuccessfulPredicate() {
        return new Predicate<PaxosResponse>() {
            @Override
            public boolean apply(@Nullable PaxosResponse response) {
                return response != null && response.isSuccessful();
            }
        };
    }
}
