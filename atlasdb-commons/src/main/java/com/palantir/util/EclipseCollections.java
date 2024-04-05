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

package com.palantir.util;

import com.google.common.base.Stopwatch;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.collections.api.factory.primitive.LongLists;

public final class EclipseCollections {
    private static final SafeLogger log = SafeLoggerFactory.get(EclipseCollections.class);

    private static final AtomicBoolean isLoaded = new AtomicBoolean(false);

    /**
     * Eagerly load Eclipse collections classes to avoid potential JDK classloader deadlock bug
     * <a href="https://bugs.openjdk.org/browse/JDK-8266350">JDK-8266350</a> when one thread is attempting to load a
     * native library (e.g. netty which is used by grpc) while another thread is attempting to load classes from a
     * cryptographically signed JAR (e.g. eclipse-collections). This has been seen by some folks using netty &
     * eclipse-collections in <a href="https://github.com/netty/netty/issues/11209#issuecomment-829468638">
     * netty issue 11209</a>.
     * <p>
     * <a href="https://bugs.openjdk.org/browse/JDK-8266310">JDK-8266310</a> fixed this deadlock in JDK 18+.
     */
    public static void loadClasses() {
        if (!isLoaded.compareAndSet(false, true)) {
            log.debug("eclipse-collections classes already loaded");
            return;
        }

        if (Runtime.version().feature() > 17) {
            log.debug("Running JDK not impacted by JDK-8266350", SafeArg.of("version", Runtime.version()));
            return; // classloader deadlock fixed in JDK 18+ by JDK-8266310
        }

        Stopwatch timer = Stopwatch.createStarted();
        Preconditions.checkState(LongLists.immutable.of().isEmpty(), "Could not load immutable LongList");

        // preload classes that traverse ServiceLoader
        List.of(
                        "org.eclipse.collections.api.block.function.primitive.DoubleToIntFunction",
                        "org.eclipse.collections.api.block.function.primitive.IntFunction",
                        "org.eclipse.collections.api.block.function.primitive.IntToIntFunction",
                        "org.eclipse.collections.api.block.function.primitive.LongToIntFunction",
                        "org.eclipse.collections.api.block.procedure.primitive.LongIntProcedure",
                        "org.eclipse.collections.api.block.procedure.primitive.ObjectIntProcedure",
                        "org.eclipse.collections.api.factory.Bags",
                        "org.eclipse.collections.api.factory.BiMaps",
                        "org.eclipse.collections.api.factory.Lists",
                        "org.eclipse.collections.api.factory.Maps",
                        "org.eclipse.collections.api.factory.Sets",
                        "org.eclipse.collections.api.factory.SortedBags",
                        "org.eclipse.collections.api.factory.SortedMaps",
                        "org.eclipse.collections.api.factory.SortedSets",
                        "org.eclipse.collections.api.factory.Stacks",
                        "org.eclipse.collections.api.factory.primitive.BooleanBags",
                        "org.eclipse.collections.api.factory.primitive.BooleanLists",
                        "org.eclipse.collections.api.factory.primitive.BooleanSets",
                        "org.eclipse.collections.api.factory.primitive.BooleanStacks",
                        "org.eclipse.collections.api.factory.primitive.ByteBags",
                        "org.eclipse.collections.api.factory.primitive.ByteBooleanMaps",
                        "org.eclipse.collections.api.factory.primitive.ByteByteMaps",
                        "org.eclipse.collections.api.factory.primitive.ByteCharMaps",
                        "org.eclipse.collections.api.factory.primitive.ByteDoubleMaps",
                        "org.eclipse.collections.api.factory.primitive.ByteFloatMaps",
                        "org.eclipse.collections.api.factory.primitive.ByteIntMaps",
                        "org.eclipse.collections.api.factory.primitive.ByteLists",
                        "org.eclipse.collections.api.factory.primitive.ByteLongMaps",
                        "org.eclipse.collections.api.factory.primitive.ByteObjectMaps",
                        "org.eclipse.collections.api.factory.primitive.ByteSets",
                        "org.eclipse.collections.api.factory.primitive.ByteShortMaps",
                        "org.eclipse.collections.api.factory.primitive.ByteStacks",
                        "org.eclipse.collections.api.factory.primitive.CharBags",
                        "org.eclipse.collections.api.factory.primitive.CharBooleanMaps",
                        "org.eclipse.collections.api.factory.primitive.CharByteMaps",
                        "org.eclipse.collections.api.factory.primitive.CharCharMaps",
                        "org.eclipse.collections.api.factory.primitive.CharDoubleMaps",
                        "org.eclipse.collections.api.factory.primitive.CharFloatMaps",
                        "org.eclipse.collections.api.factory.primitive.CharIntMaps",
                        "org.eclipse.collections.api.factory.primitive.CharLists",
                        "org.eclipse.collections.api.factory.primitive.CharLongMaps",
                        "org.eclipse.collections.api.factory.primitive.CharObjectMaps",
                        "org.eclipse.collections.api.factory.primitive.CharSets",
                        "org.eclipse.collections.api.factory.primitive.CharShortMaps",
                        "org.eclipse.collections.api.factory.primitive.CharStacks",
                        "org.eclipse.collections.api.factory.primitive.DoubleBags",
                        "org.eclipse.collections.api.factory.primitive.DoubleBooleanMaps",
                        "org.eclipse.collections.api.factory.primitive.DoubleByteMaps",
                        "org.eclipse.collections.api.factory.primitive.DoubleCharMaps",
                        "org.eclipse.collections.api.factory.primitive.DoubleDoubleMaps",
                        "org.eclipse.collections.api.factory.primitive.DoubleFloatMaps",
                        "org.eclipse.collections.api.factory.primitive.DoubleIntMaps",
                        "org.eclipse.collections.api.factory.primitive.DoubleLists",
                        "org.eclipse.collections.api.factory.primitive.DoubleLongMaps",
                        "org.eclipse.collections.api.factory.primitive.DoubleObjectMaps",
                        "org.eclipse.collections.api.factory.primitive.DoubleSets",
                        "org.eclipse.collections.api.factory.primitive.DoubleShortMaps",
                        "org.eclipse.collections.api.factory.primitive.DoubleStacks",
                        "org.eclipse.collections.api.factory.primitive.FloatBags",
                        "org.eclipse.collections.api.factory.primitive.FloatBooleanMaps",
                        "org.eclipse.collections.api.factory.primitive.FloatByteMaps",
                        "org.eclipse.collections.api.factory.primitive.FloatCharMaps",
                        "org.eclipse.collections.api.factory.primitive.FloatDoubleMaps",
                        "org.eclipse.collections.api.factory.primitive.FloatFloatMaps",
                        "org.eclipse.collections.api.factory.primitive.FloatIntMaps",
                        "org.eclipse.collections.api.factory.primitive.FloatLists",
                        "org.eclipse.collections.api.factory.primitive.FloatLongMaps",
                        "org.eclipse.collections.api.factory.primitive.FloatObjectMaps",
                        "org.eclipse.collections.api.factory.primitive.FloatSets",
                        "org.eclipse.collections.api.factory.primitive.FloatShortMaps",
                        "org.eclipse.collections.api.factory.primitive.FloatStacks",
                        "org.eclipse.collections.api.factory.primitive.IntBags",
                        "org.eclipse.collections.api.factory.primitive.IntBooleanMaps",
                        "org.eclipse.collections.api.factory.primitive.IntByteMaps",
                        "org.eclipse.collections.api.factory.primitive.IntCharMaps",
                        "org.eclipse.collections.api.factory.primitive.IntDoubleMaps",
                        "org.eclipse.collections.api.factory.primitive.IntFloatMaps",
                        "org.eclipse.collections.api.factory.primitive.IntIntMaps",
                        "org.eclipse.collections.api.factory.primitive.IntLists",
                        "org.eclipse.collections.api.factory.primitive.IntLongMaps",
                        "org.eclipse.collections.api.factory.primitive.IntObjectMaps",
                        "org.eclipse.collections.api.factory.primitive.IntSets",
                        "org.eclipse.collections.api.factory.primitive.IntShortMaps",
                        "org.eclipse.collections.api.factory.primitive.IntStacks",
                        "org.eclipse.collections.api.factory.primitive.LongBags",
                        "org.eclipse.collections.api.factory.primitive.LongBooleanMaps",
                        "org.eclipse.collections.api.factory.primitive.LongByteMaps",
                        "org.eclipse.collections.api.factory.primitive.LongCharMaps",
                        "org.eclipse.collections.api.factory.primitive.LongDoubleMaps",
                        "org.eclipse.collections.api.factory.primitive.LongFloatMaps",
                        "org.eclipse.collections.api.factory.primitive.LongIntMaps",
                        "org.eclipse.collections.api.factory.primitive.LongLists",
                        "org.eclipse.collections.api.factory.primitive.LongLongMaps",
                        "org.eclipse.collections.api.factory.primitive.LongObjectMaps",
                        "org.eclipse.collections.api.factory.primitive.LongSets",
                        "org.eclipse.collections.api.factory.primitive.LongShortMaps",
                        "org.eclipse.collections.api.factory.primitive.LongStacks",
                        "org.eclipse.collections.api.factory.primitive.ObjectBooleanHashingStrategyMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectBooleanMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectByteHashingStrategyMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectByteMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectCharHashingStrategyMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectCharMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectDoubleHashingStrategyMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectDoubleMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectFloatHashingStrategyMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectFloatMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectIntHashingStrategyMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectIntMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectLongHashingStrategyMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectLongMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectShortHashingStrategyMaps",
                        "org.eclipse.collections.api.factory.primitive.ObjectShortMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortBags",
                        "org.eclipse.collections.api.factory.primitive.ShortBooleanMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortByteMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortCharMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortDoubleMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortFloatMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortIntMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortLists",
                        "org.eclipse.collections.api.factory.primitive.ShortLongMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortObjectMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortSets",
                        "org.eclipse.collections.api.factory.primitive.ShortShortMaps",
                        "org.eclipse.collections.api.factory.primitive.ShortStacks",
                        "org.eclipse.collections.api.iterator.LongIterator",
                        "org.eclipse.collections.api.list.primitive.MutableLongList",
                        "org.eclipse.collections.api.set.primitive.LongSet",
                        "org.eclipse.collections.api.tuple.primitive.DoubleIntPair",
                        "org.eclipse.collections.api.tuple.primitive.IntIntPair",
                        "org.eclipse.collections.api.tuple.primitive.LongIntPair",
                        "org.eclipse.collections.api.tuple.primitive.ObjectIntPair",
                        "org.eclipse.collections.impl.map.mutable.primitive.DoubleIntHashMap",
                        "org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap",
                        "org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap",
                        "org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap",
                        "org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap",
                        "org.eclipse.collections.impl.set.mutable.primitive.LongHashSet",
                        "org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples")
                .forEach(EclipseCollections::initializeClass);

        log.info(
                "Initialized eclipse-collections classes most likely impacted by JDK-8266350",
                SafeArg.of("duration", timer.stop()),
                SafeArg.of("version", Runtime.version()));
    }

    private static void initializeClass(String name) {
        try {
            Class.forName(name, /* initialize= */ true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            log.error("Unable to eagerly initialize class", SafeArg.of("class", name), e);
        }
    }

    private EclipseCollections() {}
}
