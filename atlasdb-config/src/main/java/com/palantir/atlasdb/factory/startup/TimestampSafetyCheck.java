/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.factory.startup;

import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.safetycheck.ServerKiller;
import com.palantir.lock.v2.TimelockService;

/**
 * A TimestampSafetyCheck's check() method attempts to get a fresh timestamp from a timelock service, and compares
 * this against a Supplier that supplies lower bounds on the fresh timestamp.
 *
 * In the event that the check fails, we invoke the ServerKiller to stop the server.
 * The aim of this class is to limit, as far as possible, the timeframe where AtlasDB is live and yet we know that
 * we have gone back in time.
 */
public class TimestampSafetyCheck {
    private static final Logger log = LoggerFactory.getLogger(TimestampSafetyCheck.class);

    private final TimelockService timelockService;
    private final Supplier<Long> lowerBoundSupplier;
    private final StrictnessMode strictnessMode;

    private TimestampSafetyCheck(
            TimelockService timelockService,
            Supplier<Long> lowerBoundSupplier,
            StrictnessMode strictnessMode) {
        this.timelockService = timelockService;
        this.lowerBoundSupplier = lowerBoundSupplier;
        this.strictnessMode = strictnessMode;
    }

    public static TimestampSafetyCheck create(
            TimelockService timelockService,
            Cleaner cleaner,
            AtlasDbConfig config) {
        // TODO (jkong): Leverage the immutable timestamp to create an even stricter check?
        return new TimestampSafetyCheck(
                timelockService,
                cleaner::getUnreadableTimestamp, // this is a decent best effort approximation
                config.timelock().isPresent() ? StrictnessMode.STRICT : StrictnessMode.LENIENT);
        // Note that without timelock we need to be lenient, because if we are strict then we can't bootstrap at all.
    }

    public void check() {
        try {
            checkInternal();
        } catch (Exception e) {
            ServerKiller.kill(e);
        }
    }

    private void checkInternal() {
        Optional<Long> freshTimestamp = getOptionalTimestamp();
        if (!freshTimestamp.isPresent()) {
            if (strictnessMode == StrictnessMode.STRICT) {
                throw new IllegalStateException("We couldn't check that the timestamp service was sane, because we"
                        + " couldn't get a timestamp from it!");
            }
            log.info("Safety check passed, because we were in lenient mode and we couldn't get a timestamp.");
            return;
        }
        checkTimestampAgainstLowerBound(freshTimestamp.get());
    }

    private void checkTimestampAgainstLowerBound(long timestamp) {
        long lowerBound = lowerBoundSupplier.get();
        if (timestamp <= lowerBound) {
            String errorMessage = String.format(
                    "AtlasDB appears to have gone back in time!"
                            + " We observed a fresh timestamp of [%d], not higher than the Unreadable Timestamp [%d]."
                            + " This may have occurred if the timestamp client was changed, or if the timelock"
                            + " server lost its logs.",
                    timestamp,
                    lowerBound);
            throw new IllegalStateException(errorMessage);
        }
        log.info("Safety check passed, because we observed a fresh timestamp of [%d] which is greater than the"
                + " Unreadable Timestamp of [%d].",
                timestamp,
                lowerBound);
    }

    private Optional<Long> getOptionalTimestamp() {
        try {
            return Optional.of(timelockService.getFreshTimestamp());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public enum StrictnessMode {
        STRICT, // we must get a timestamp, or we kill the server
        LENIENT // we can proceed even if we couldn't get a timestamp
    }
}
