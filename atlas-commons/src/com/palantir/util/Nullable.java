package com.palantir.util;

import java.util.Objects;

import javax.annotation.CheckForNull;

/**
 * Represents a reference to a value that
 * can potentially be null.  Gives nicer
 * semantics and guarantees around handling
 * null values.
 * @author mharris
 */
public final class Nullable<T> {

    @CheckForNull
    private final T reference;

    /**
     * Constructs a new Nullable holding
     * the provided reference.  <code>reference</code>
     * may not be null.
     * @param reference The reference to use
     * @return A Nullable holding the provided
     * reference.
     */
    public static <T> Nullable<T> of(T reference) {
        if (reference == null) {
            throw new IllegalArgumentException("Reference may not be null.");
        }
        return new Nullable<T>(reference);
    }

    /**
     * Constructs a new Nullable holding
     * the provided reference.  It is legal for
     * <code>reference</code> to be null.
     * @param reference The reference to use
     * @return A Nullable holding the provided
     * reference.
     */
    public static <T> Nullable<T> ofNullable(@CheckForNull T reference) {
        return new Nullable<T>(reference);
    }

    private Nullable(T reference) {
        this.reference = reference;
    }

    /**
     * Returns this Nullable's reference if it is
     * non-null.  Throws an <code>IllegalStateException</code>
     * if the reference is null
     * @return The reference this Nullable holds
     */
    public T get() {
        if(reference == null) {
            throw new IllegalStateException("Internal reference was null!");
        }
        return reference;
    }

    /**
     * Returns this Nullable's reference, which
     * may be null
     * @return The reference this Nullable holds
     */
    @CheckForNull
    public T orNull() {
        return reference;
    }

    /**
     * Returns this Nullable's reference if it is
     * non-null.  Otherwise returns <code>defaultValue</code>
     * @return The reference this Nullable holds if
     * it is non-null, defaultValue otherwise.
     */
    public T or(T defaultValue) {
        if (reference == null) {
            return defaultValue;
        }
        return reference;
    }

    /**
     * Whether this Nullable holds a non-null reference
     * @return true if this Nullable holds a
     * non-null reference, false otherwise.
     */
    public boolean isPresent() {
        return reference != null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((reference == null) ? 0 : reference.hashCode());
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
        Nullable<?> other = (Nullable<?>) obj;
        if (!Objects.equals(this.reference, other.reference)) {
            return false;
        }
        return true;
    }

    /**
     * Returns a Nullable holding a null reference.
     * @return A Nullable holding a null reference.
     */
    public static <T> Nullable<T> absent() {
        return new Nullable<T>(null);
    }

    @Override
    public String toString() {
        return String.valueOf(reference);
    }
}
