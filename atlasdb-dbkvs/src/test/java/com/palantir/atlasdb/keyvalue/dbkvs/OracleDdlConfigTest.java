/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class OracleDdlConfigTest {
    private static final String ACCEPTED_PREFIX = "a_";
    private static final String ACCEPTED_OVERFLOW_PREFIX = "ao_";

    @Test
    public void standardTableAndOverflowPrefixesAreAccepted() {
        assertThat(createLegacyCompatibleOracleConfigWithPrefixes(ACCEPTED_PREFIX, ACCEPTED_OVERFLOW_PREFIX))
                .satisfies(config -> {
                    assertThat(config.tablePrefix()).isEqualTo(ACCEPTED_PREFIX);
                    assertThat(config.overflowTablePrefix()).isEqualTo(ACCEPTED_OVERFLOW_PREFIX);
                });
    }

    @Test
    public void tablePrefixesMustEndWithUnderscoreOrDollarSign() {
        assertThatThrownBy(() -> createLegacyCompatibleOracleConfigWithPrefixes("a", ACCEPTED_OVERFLOW_PREFIX))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("'tablePrefix' must end with an underscore or a dollar sign");
        assertThatCode(() -> createLegacyCompatibleOracleConfigWithPrefixes("a$", ACCEPTED_OVERFLOW_PREFIX))
                .doesNotThrowAnyException();
    }

    @Test
    public void tablePrefixesMustNotBeginWithUnderscore() {
        assertThatThrownBy(() -> createLegacyCompatibleOracleConfigWithPrefixes("_t", ACCEPTED_OVERFLOW_PREFIX))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("'tablePrefix' cannot begin with underscore");
    }

    @Test
    public void overflowTablePrefixesMustEndWithUnderscoreOrDollarSign() {
        assertThatThrownBy(() -> createLegacyCompatibleOracleConfigWithPrefixes(ACCEPTED_PREFIX, "ao"))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("'overflowTablePrefix' must end with an underscore or a dollar sign");
        assertThatCode(() -> createLegacyCompatibleOracleConfigWithPrefixes(ACCEPTED_PREFIX, "b$"))
                .doesNotThrowAnyException();
    }

    @Test
    public void overflowTablePrefixesMustNotBeginWithUnderscore() {
        assertThatThrownBy(() -> createLegacyCompatibleOracleConfigWithPrefixes(ACCEPTED_PREFIX, "_p"))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("'overflowTablePrefix' cannot begin with underscore");
    }

    @Test
    public void overflowPrefixHasMaximumLengthSix() {
        assertThatCode(() ->
                        createLegacyCompatibleOracleConfigWithPrefixes(getPrefixWithLength(7), getPrefixWithLength(6)))
                .doesNotThrowAnyException();
        assertThatThrownBy(() ->
                        createLegacyCompatibleOracleConfigWithPrefixes(getPrefixWithLength(7), getPrefixWithLength(7)))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("'overflowTablePrefix' exceeds the length limit")
                .hasMessageContaining("6");
    }

    @Test
    public void tablePrefixHasMaximumLengthSeven() {
        assertThatThrownBy(() ->
                        createLegacyCompatibleOracleConfigWithPrefixes(getPrefixWithLength(8), getPrefixWithLength(6)))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("'tablePrefix' exceeds the length limit")
                .hasMessageContaining("7");
    }

    @Test
    public void longPrefixesAllowedIfConfigSpecificallyAllowsThem() {
        assertThatCode(() -> createLongNameSupportingOracleConfigWithPrefixes(
                        getPrefixWithLength(42), getPrefixWithLength(41)))
                .doesNotThrowAnyException();
        assertThatCode(() -> createLongNameSupportingOracleConfigWithPrefixes(
                        getPrefixWithLength(41), getPrefixWithLength(42)))
                .doesNotThrowAnyException();
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored") // We're interested in whether this throws an exception or not!
    public void tableMappingAndLongNameSupportNotAllowedTogether() {
        assertThatThrownBy(() -> ImmutableOracleDdlConfig.builder()
                        .tablePrefix(getPrefixWithLength(5))
                        .overflowTablePrefix(getPrefixWithLength(4))
                        .overflowMigrationState(OverflowMigrationState.FINISHED)
                        .longIdentifierNamesSupported(true)
                        .useTableMapping(true)
                        .build())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("The table mapper does not support long identifier names");
    }

    @Test
    public void excessivelyLongPrefixesStillDisallowedEvenWithLongNameSupport() {
        assertThatThrownBy(() -> createLongNameSupportingOracleConfigWithPrefixes(
                        getPrefixWithLength(57), getPrefixWithLength(14)))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("'tablePrefix' exceeds the length limit")
                .hasMessageContaining("56");
        assertThatThrownBy(() -> createLongNameSupportingOracleConfigWithPrefixes(
                        getPrefixWithLength(22), getPrefixWithLength(57)))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("'overflowTablePrefix' exceeds the length limit")
                .hasMessageContaining("56");
    }

    @Test
    public void tableMappingIsByDefaultOn() {
        assertThat(ImmutableOracleDdlConfig.builder()
                        .overflowMigrationState(OverflowMigrationState.FINISHED)
                        .build()
                        .useTableMapping())
                .isTrue();
    }

    @Test
    public void canSetTableMappingExplicitly() {
        List<Boolean> values = ImmutableList.of(false, true);
        for (boolean value : values) {
            assertThat(ImmutableOracleDdlConfig.builder()
                            .overflowMigrationState(OverflowMigrationState.FINISHED)
                            .forceTableMapping(value)
                            .build()
                            .useTableMapping())
                    .isEqualTo(value);
        }
    }

    @Test
    public void serializedFormDoesNotIncludeLengthLimits() throws JsonProcessingException {
        JsonMapper jsonMapper = ObjectMappers.newServerJsonMapper();

        OracleDdlConfig config =
                createLongNameSupportingOracleConfigWithPrefixes(getPrefixWithLength(8), getPrefixWithLength(15));
        assertThat(jsonMapper.writeValueAsString(config)).doesNotContain("identifierLengthLimits");
    }

    @Test
    public void serializeAndDeserializeIsNoOp() throws JsonProcessingException {
        JsonMapper jsonMapper = ObjectMappers.newServerJsonMapper();

        OracleDdlConfig config =
                createLongNameSupportingOracleConfigWithPrefixes(getPrefixWithLength(44), getPrefixWithLength(33));
        assertThat(jsonMapper.readValue(jsonMapper.writeValueAsString(config), OracleDdlConfig.class))
                .isEqualTo(config);
    }

    private static OracleDdlConfig createLegacyCompatibleOracleConfigWithPrefixes(
            String tablePrefix, String overflowTablePrefix) {
        return createOracleDdlConfig(tablePrefix, overflowTablePrefix, false);
    }

    private static OracleDdlConfig createLongNameSupportingOracleConfigWithPrefixes(
            String tablePrefix, String overflowTablePrefix) {
        return createOracleDdlConfig(tablePrefix, overflowTablePrefix, true);
    }

    private static ImmutableOracleDdlConfig createOracleDdlConfig(
            String tablePrefix, String overflowTablePrefix, boolean longIdentifierNamesSupported) {
        return ImmutableOracleDdlConfig.builder()
                .tablePrefix(tablePrefix)
                .overflowTablePrefix(overflowTablePrefix)
                .overflowMigrationState(OverflowMigrationState.FINISHED)
                .longIdentifierNamesSupported(longIdentifierNamesSupported)
                .forceTableMapping(false)
                .build();
    }

    private static String getPrefixWithLength(int length) {
        return String.join("", Collections.nCopies(length - 1, "a")) + "_";
    }
}
