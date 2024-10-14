/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.conjure.java.serialization.ObjectMappers;

/**
 * The object mapper present in this class is configured to ensure that, given the same input object, the serialised
 * form is _always_ syntactically the same. This is important when performing a CAS operation from one present value
 * to another, where the value is a serialised object. If the serialised form changes, the CAS operation will fail.
 *
 * Moreover, in the presence of new Optional fields that are empty, the serialised form will be the same as if the
 * field was not present at all. This is important for backwards compatibility - when adding new fields to the
 * object, must still be able to retrieve the old serialised format of the object for CAS operations over old values.
 *
 * However, the object mapper does not explicitly sort collections of any kind. Clients using this object mapper must
 * take care to ensure that collections are sorted before serialisation to ensure consistent serialisation.
 * (Or, implement a custom serialiser that sorts collections containing Comparables, and register it with this
 * ObjectMapper - this was not done at the time of writing because it was not necessary.)
 *
 * In essence:
 * + Orders properties consistently
 * + Orders map keys consistently
 * + Empty optionals / atomic references / nulls are treated as if the field was not present
 * + Does not fail on unknown properties
 *
 * - Does not sort collections / streams e.t.c
 */
public final class ConsistentOrderingObjectMappers {
    private ConsistentOrderingObjectMappers() {
        // utility
    }

    public static final ObjectMapper OBJECT_MAPPER =
            configureConsistentOrderingObjectMapper(ObjectMappers.newClientSmileMapper());

    @VisibleForTesting // Used to test a JSON variant of this, since it's much easier to handroll JSON!
    static ObjectMapper configureConsistentOrderingObjectMapper(ObjectMapper baseMapper) {
        return baseMapper
                .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                // FAIL_ON_UNKNOWN_PROPERTIES should already be disabled by using the client object mapper, but
                // disabling it here just to make sure it doesn't get changed from underneath us.
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(Include.NON_ABSENT);
    }
}
