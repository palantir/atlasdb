package com.palantir.atlas.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TransactionToken {
    private static final String AUTO_COMMIT = "auto-commit";
    private final String id;

    @JsonCreator
    public TransactionToken(@JsonProperty("id") String id) {
        this.id = id;
    }

    public static TransactionToken autoCommit() {
        return new TransactionToken(AUTO_COMMIT);
    }

    public String getId() {
        return id;
    }

    public boolean shouldAutoCommit() {
        return id.equals(AUTO_COMMIT);
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        TransactionToken other = (TransactionToken) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        return true;
    }
}
