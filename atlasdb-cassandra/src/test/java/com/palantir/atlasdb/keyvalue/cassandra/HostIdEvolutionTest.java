/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

public class HostIdEvolutionTest {

    @Test
    public void zeroSetsAreAllConnectedByNonemptyIntersections() {
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of()))
                .isTrue();
    }

    @Test
    public void singleNonemptySetConnectedToItselfByNonemptyIntersections() {
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of(ImmutableSet.of("apple"))))
                .isTrue();
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(
                        ImmutableSet.of(ImmutableSet.of("broccoli", "carrot", "durian"))))
                .isTrue();
    }

    @Test
    public void singleEmptySetNotConnectedToItselfByNonemptyIntersections() {
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of(ImmutableSet.of())))
                .isFalse();
    }

    @Test
    public void twoIntersectingSetsConnectedByNonemptyIntersections() {
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of(
                        ImmutableSet.of("eggplant", "fig", "grapefruit"),
                        ImmutableSet.of("eggplant", "fig", "grapefruit", "honeydew"))))
                .isTrue();
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of(
                        ImmutableSet.of("iceberg", "jicama", "kale"), ImmutableSet.of("kale", "leek", "mushroom"))))
                .isTrue();
    }

    @Test
    public void twoDisjointSetsNotConnectedByNonemptyIntersections() {
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of(
                        ImmutableSet.of("nectarine", "orange", "pear"), ImmutableSet.of("quince", "rhubarb"))))
                .isFalse();
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(
                        ImmutableSet.of(ImmutableSet.of("samphire", "taro", "ume"), ImmutableSet.of())))
                .isFalse();
    }

    @Test
    public void multipleSometimesPairwiseDisjointSetsConnectedByNonemptyIntersections() {
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of(
                        ImmutableSet.of("vanilla", "walnut"),
                        ImmutableSet.of("walnut", "xocolatl"),
                        ImmutableSet.of("xocolatl", "yam"),
                        ImmutableSet.of("yam", "zucchini"),
                        ImmutableSet.of("zucchini", "almond"))))
                .isTrue();
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of(
                        ImmutableSet.of("beetroot", "currant"),
                        ImmutableSet.of("broccoli", "currant"),
                        ImmutableSet.of("currant", "durian"),
                        ImmutableSet.of("durian", "eggplant"),
                        ImmutableSet.of("durian", "edamame"))))
                .isTrue();
    }

    @Test
    public void multipleDisjointComponentsNotConnectedByNonemptyIntersections() {
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of(
                        ImmutableSet.of("apple", "banana"),
                        ImmutableSet.of("banana", "carrot"),
                        ImmutableSet.of("denim", "eggplant"),
                        ImmutableSet.of("eggplant", "fig"))))
                .isFalse();
        assertThat(HostIdEvolution.existsPlausibleEvolutionOfHostIdSets(ImmutableSet.of(
                        ImmutableSet.of("iceberg"),
                        ImmutableSet.of("jerusalem artichoke", "kale"),
                        ImmutableSet.of("kale", "leek"))))
                .isFalse();
    }
}
