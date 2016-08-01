package com.palantir.nexus.db.pool.config;

import org.immutables.value.Value;

@Value.Immutable(builder = false)
public abstract class PasswordValue {
    @Value.Parameter
    abstract String value();
    @Override
    public String toString() { return "<REDACTED>"; }
}
