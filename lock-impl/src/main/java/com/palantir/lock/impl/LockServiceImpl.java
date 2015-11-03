/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.lock.impl;

import static com.palantir.lock.BlockingMode.BLOCK_UNTIL_TIMEOUT;
import static com.palantir.lock.BlockingMode.DO_NOT_BLOCK;
import static com.palantir.lock.LockClient.INTERNAL_LOCK_GRANT_CLIENT;
import static com.palantir.lock.LockGroupBehavior.LOCK_ALL_OR_NONE;
import static com.palantir.lock.LockGroupBehavior.LOCK_AS_MANY_AS_POSSIBLE;

import java.io.Closeable;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedMap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultiset;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.random.SecureRandomPool;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.lock.BlockingMode;
import com.palantir.lock.ExpiringToken;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.HeldLocksTokens;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollection;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockGroupBehavior;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.SortedLockCollection;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.TimeDuration;
import com.palantir.util.JMXUtils;
import com.palantir.util.Pair;

/**
 * Implementation of the Lock Server.
 *
 * @author jtamer
 */
@ThreadSafe public final class LockServiceImpl implements LockService, RemoteLockService, LockServiceImplMBean, Closeable {

    private static final Logger log = LoggerFactory.getLogger(LockServiceImpl.class);
    private static final Logger requestLogger = LoggerFactory.getLogger("lock.request");

    /** Executor for the reaper threads. */
    private final ExecutorService executor = PTExecutors.newCachedThreadPool(
            new NamedThreadFactory(LockServiceImpl.class.getName(), true));

    private static final Function<HeldLocksToken, String> TOKEN_TO_ID =
            new Function<HeldLocksToken, String>() {
        @Override
        public String apply(HeldLocksToken from) {
            return from.getTokenId().toString(Character.MAX_RADIX);
        }
    };

    /**
     * A set of locks held by the lock server, along with the canonical
     * {@link HeldLocksToken} or {@link HeldLocksGrant} object for these locks.
     */
    @Immutable private static class HeldLocks<T extends ExpiringToken> {
        final T realToken;
        final LockCollection<? extends ClientAwareReadWriteLock> locks;

        static <T extends ExpiringToken> HeldLocks<T> of(T token,
                LockCollection<? extends ClientAwareReadWriteLock> locks) {
            return new HeldLocks<T>(token, locks);
        }

        HeldLocks(T token, LockCollection<? extends ClientAwareReadWriteLock> locks) {
            this.realToken = Preconditions.checkNotNull(token);
            this.locks = locks;
        }

        @Override public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("realToken", realToken)
                    .add("locks", locks)
                    .toString();
        }
    }

    public static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
    public static final int SECURE_RANDOM_POOL_SIZE = 100;
    private final SecureRandomPool randomPool = new SecureRandomPool(SECURE_RANDOM_ALGORITHM, SECURE_RANDOM_POOL_SIZE);

    private final boolean isStandaloneServer;
    private final TimeDuration maxAllowedLockTimeout;
    private final TimeDuration maxAllowedClockDrift;
    private final TimeDuration maxAllowedBlockingDuration;
    private final TimeDuration maxNormalLockAge;
    private final int randomBitCount;
    private final Runnable callOnClose;
    private volatile boolean isShutDown = false;

    private final LockClientIndices clientIndices = new LockClientIndices();

    /** The backing client-aware read write lock for each lock descriptor. */
    private final LoadingCache<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap =
            CacheBuilder.newBuilder().weakValues().build(
                    new CacheLoader<LockDescriptor, ClientAwareReadWriteLock>() {
                        @Override
                        public ClientAwareReadWriteLock load(LockDescriptor from) {
                            return new LockServerLock(from, clientIndices);
                        }
                    });

    /** The locks (and canonical token) associated with each HeldLocksToken. */
    private final ConcurrentMap<HeldLocksToken, HeldLocks<HeldLocksToken>> heldLocksTokenMap =
            new MapMaker().makeMap();

    /** The locks (and canonical token) associated with each HeldLocksGrant. */
    private final ConcurrentMap<HeldLocksGrant, HeldLocks<HeldLocksGrant>> heldLocksGrantMap =
            new MapMaker().makeMap();

    /** The priority queue of lock tokens waiting to be reaped. */
    private final BlockingQueue<HeldLocksToken> lockTokenReaperQueue =
            new PriorityBlockingQueue<HeldLocksToken>(1, ExpiringToken.COMPARATOR);

    /** The priority queue of lock grants waiting to be reaped. */
    private final BlockingQueue<HeldLocksGrant> lockGrantReaperQueue =
            new PriorityBlockingQueue<HeldLocksGrant>(1, ExpiringToken.COMPARATOR);

    /** The mapping from lock client to the set of tokens held by that client. */
    private final SetMultimap<LockClient, HeldLocksToken> lockClientMultimap =
            Multimaps.synchronizedSetMultimap(HashMultimap.<LockClient, HeldLocksToken>create());

    private final SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap =
        Multimaps.synchronizedSetMultimap(HashMultimap.<LockClient, LockRequest>create());

    private final Multimap<LockClient, Long> versionIdMap = Multimaps.synchronizedMultimap(
            Multimaps.newMultimap(Maps.<LockClient, Collection<Long>>newHashMap(), new Supplier<TreeMultiset<Long>>() {
                @Override
                public TreeMultiset<Long> get() {
                    return TreeMultiset.create();
                }
            }));

    private static final AtomicInteger instanceCount = new AtomicInteger();
    private static final int MAX_FAILED_LOCKS_TO_LOG = 20;
    private static final int MAX_LOCKS_TO_LOG = 10000;

    /** Creates a new lock server instance with default options. */
    // TODO (jtamer) read lock server options from a prefs file
    public static LockServiceImpl create() {
        return create(LockServerOptions.DEFAULT);
    }

    /** Creates a new lock server instance with the given options. */
    public static LockServiceImpl create(LockServerOptions options) {
        if (log.isTraceEnabled()) {
            log.trace("Creating LockService with options=" + options);
        }
        final String jmxBeanRegistrationName = "com.palantir.lock:type=LockServer_" + instanceCount.getAndIncrement();
        LockServiceImpl lockService = new LockServiceImpl(options, new Runnable() {
            @Override public void run() {
                JMXUtils.unregisterMBeanCatchAndLogExceptions(jmxBeanRegistrationName);
            }
        });
        JMXUtils.registerMBeanCatchAndLogExceptions(lockService, jmxBeanRegistrationName);
        return lockService;
    }

    private LockServiceImpl(LockServerOptions options, Runnable callOnClose) {
        Preconditions.checkNotNull(options);
        this.callOnClose = callOnClose;
        isStandaloneServer = options.isStandaloneServer();
        maxAllowedLockTimeout = SimpleTimeDuration.of(options.getMaxAllowedLockTimeout());
        maxAllowedClockDrift = SimpleTimeDuration.of(options.getMaxAllowedClockDrift());
        maxAllowedBlockingDuration = SimpleTimeDuration.of(options.getMaxAllowedBlockingDuration());
        maxNormalLockAge = SimpleTimeDuration.of(options.getMaxNormalLockAge());
        randomBitCount = options.getRandomBitCount();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("Held Locks Token Reaper");
                reapLocks(lockTokenReaperQueue, heldLocksTokenMap);
            }
        });
        executor.execute(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("Held Locks Grant Reaper");
                reapLocks(lockGrantReaperQueue, heldLocksGrantMap);
            }
        });
    }

    private HeldLocksToken createHeldLocksToken(LockClient client,
            SortedLockCollection<LockDescriptor> lockDescriptorMap,
            LockCollection<? extends ClientAwareReadWriteLock> heldLocksMap, TimeDuration lockTimeout,
            @Nullable Long versionId) {
        while (true) {
            BigInteger tokenId = new BigInteger(randomBitCount, randomPool.getSecureRandom());
            long expirationDateMs = currentTimeMillis() + lockTimeout.toMillis();
            HeldLocksToken token = new HeldLocksToken(tokenId, client, currentTimeMillis(),
                    expirationDateMs, lockDescriptorMap, lockTimeout, versionId);
            HeldLocks<HeldLocksToken> heldLocks = HeldLocks.of(token, heldLocksMap);
            if (heldLocksTokenMap.putIfAbsent(token, heldLocks) == null) {
                lockTokenReaperQueue.add(token);
                if (!client.isAnonymous()) {
                    lockClientMultimap.put(client, token);
                }
                return token;
            }
            log.error("Lock ID collision! The RANDOM_BIT_COUNT constant must be increased. "
                    + "Count of held tokens = " + heldLocksTokenMap.size()
                    + "; random bit count = " + randomBitCount);
        }
    }

    private HeldLocksGrant createHeldLocksGrant(SortedLockCollection<LockDescriptor> lockDescriptorMap,
            LockCollection<? extends ClientAwareReadWriteLock> heldLocksMap, TimeDuration lockTimeout,
            @Nullable Long versionId) {
        while (true) {
            BigInteger grantId = new BigInteger(randomBitCount, randomPool.getSecureRandom());
            long expirationDateMs = currentTimeMillis() + lockTimeout.toMillis();
            HeldLocksGrant grant = new HeldLocksGrant(grantId, System.currentTimeMillis(),
                    expirationDateMs, lockDescriptorMap, lockTimeout, versionId);
            HeldLocks<HeldLocksGrant> newHeldLocks = HeldLocks.of(grant, heldLocksMap);
            if (heldLocksGrantMap.putIfAbsent(grant, newHeldLocks) == null) {
                lockGrantReaperQueue.add(grant);
                return grant;
            }
            log.error("Lock ID collision! The RANDOM_BIT_COUNT constant must be increased. "
                    + "Count of held grants = " + heldLocksGrantMap.size()
                    + "; random bit count = " + randomBitCount);
        }
    }

    @Override
    public LockRefreshToken lockAnonymously(LockRequest request) throws InterruptedException {
        Preconditions.checkArgument(request.getLockGroupBehavior() == LockGroupBehavior.LOCK_ALL_OR_NONE,
                "lockAnonymously() only supports LockGroupBehavior.LOCK_ALL_OR_NONE. Consider using lockAndGetHeldLocksAnonymously().");
        LockResponse result = lock(LockClient.ANONYMOUS, request);
        return result.success() ? result.getLockRefreshToken() : null;
    }

    @Override
    public LockRefreshToken lockWithClient(String client, LockRequest request)
            throws InterruptedException {
        Preconditions.checkArgument(request.getLockGroupBehavior() == LockGroupBehavior.LOCK_ALL_OR_NONE,
                "lockWithClient() only supports LockGroupBehavior.LOCK_ALL_OR_NONE. Consider using lockAndGetHeldLocksWithClient().");
        LockResponse result = lock(LockClient.of(client), request);
        return result.success() ? result.getLockRefreshToken() : null;
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocksAnonymously(LockRequest request) throws InterruptedException {
        LockResponse result = lock(LockClient.ANONYMOUS, request);
        return result.getToken();
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocksWithClient(String client, LockRequest request) throws InterruptedException {
        LockResponse result = lock(LockClient.of(client), request);
        return result.getToken();
    }

    @Override
    public LockResponse lock(LockClient client, LockRequest request) throws InterruptedException {
        Preconditions.checkNotNull(client);
        Preconditions.checkArgument(client != INTERNAL_LOCK_GRANT_CLIENT);
        Preconditions.checkArgument(request.getLockTimeout().compareTo(maxAllowedLockTimeout) <= 0,
                "Requested lock timeout (%s) is greater than maximum allowed lock timeout (%s)",
                request.getLockTimeout(), maxAllowedLockTimeout);
        Preconditions.checkArgument((request.getBlockingMode() != BLOCK_UNTIL_TIMEOUT)
                || (request.getBlockingDuration().compareTo(maxAllowedBlockingDuration) <= 0),
                "Requested blocking duration (%s) is greater than maximum allowed blocking duration (%s)",
                request.getBlockingDuration(), maxAllowedBlockingDuration);
        long startTime = System.currentTimeMillis();
        if (requestLogger.isDebugEnabled()) {
            requestLogger.debug("LockServiceImpl processing lock request {} for requesting thread {}",
                    request, request.getCreatingThreadName());
        }
        Map<ClientAwareReadWriteLock, LockMode> locks = Maps.newLinkedHashMap();
        if (isShutDown) {
            throw new ServiceNotAvailableException("This lock server is shut down.");
        }
        try {
            outstandingLockRequestMultimap.put(client, request);
            Map<LockDescriptor, LockClient> failedLocks = Maps.newHashMap();
            @Nullable Long deadline = (request.getBlockingDuration() == null) ? null
                : System.nanoTime() + request.getBlockingDuration().toNanos();
            if (request.getBlockingMode() == BLOCK_UNTIL_TIMEOUT) {
                if (request.getLockGroupBehavior() == LOCK_AS_MANY_AS_POSSIBLE) {
                    tryLocks(client, request, DO_NOT_BLOCK, null, LOCK_AS_MANY_AS_POSSIBLE, locks,
                            failedLocks);
                }
            }
            tryLocks(client, request, request.getBlockingMode(), deadline,
                    request.getLockGroupBehavior(), locks, failedLocks);

            if (request.getBlockingMode() == BlockingMode.BLOCK_INDEFINITELY_THEN_RELEASE) {
                if (log.isTraceEnabled()) {
                    log.trace(".lock(" + client + ", " + request + ") returns null");
                }
                if (requestLogger.isDebugEnabled()) {
                    requestLogger.debug("Timed out requesting {} for requesting thread {} after {} ms",
                            request, request.getCreatingThreadName(), System.currentTimeMillis() - startTime);
                }
                return new LockResponse(failedLocks);
            }

            if (locks.isEmpty() || ((request.getLockGroupBehavior() == LOCK_ALL_OR_NONE)
                    && (locks.size() < request.getLockDescriptors().size()))) {
                if (log.isTraceEnabled()) {
                    log.trace(".lock(" + client + ", " + request + ") returns null");
                }
                if (requestLogger.isDebugEnabled()) {
                    requestLogger.debug("Failed to acquire all locks for {} for requesting thread {} after {} ms",
                            request, request.getCreatingThreadName(), System.currentTimeMillis() - startTime);
                }
                if (requestLogger.isTraceEnabled()) {
                    StringBuilder sb = new StringBuilder("Current holders of the first ").append(
                            MAX_FAILED_LOCKS_TO_LOG).append(" of ").append(failedLocks.size()).append(
                            " total failed locks were: [");
                    Iterator<Entry<LockDescriptor, LockClient>> entries = failedLocks.entrySet().iterator();
                    for (int i = 0; i < MAX_FAILED_LOCKS_TO_LOG; i++) {
                        if (entries.hasNext()) {
                            Entry<LockDescriptor, LockClient> entry = entries.next();
                            sb.append(" Lock: ").append(entry.getKey().toString()).append(
                                    ", Holder: ").append(entry.getValue().toString()).append(";");
                        }
                    }
                    sb.append(" ]");
                    requestLogger.trace(sb.toString());
                }
                return new LockResponse(null, failedLocks);
            }

            Builder<LockDescriptor, LockMode> lockDescriptorMap = ImmutableSortedMap.naturalOrder();
            for (Entry<ClientAwareReadWriteLock, LockMode> entry : locks.entrySet()) {
                lockDescriptorMap.put(entry.getKey().getDescriptor(), entry.getValue());
            }
            if (request.getVersionId() != null) {
                versionIdMap.put(client, request.getVersionId());
            }
            HeldLocksToken token = createHeldLocksToken(client, LockCollections.of(lockDescriptorMap.build()), LockCollections.of(locks),
                    request.getLockTimeout(), request.getVersionId());
            locks.clear();
            if (log.isTraceEnabled()) {
                log.trace(".lock(" + client + ", " + request + ") returns " + token);
            }
            if (Thread.interrupted()) {
                throw new InterruptedException("Interrupted while locking.");
            }
            if (requestLogger.isDebugEnabled()) {
                requestLogger.debug("Successfully acquired locks {} for requesting thread {} after {} ms",
                        request, request.getCreatingThreadName(), System.currentTimeMillis() - startTime);
            }
            return new LockResponse(token, failedLocks);
        } finally {
            outstandingLockRequestMultimap.remove(client, request);
            try {
                for (Entry<ClientAwareReadWriteLock, LockMode> entry : locks.entrySet()) {
                    entry.getKey().get(client, entry.getValue()).unlock();
                }
            } catch (Throwable e) { // (authorized)
                log.error("Internal lock server error: state has been corrupted!!", e);
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    private void tryLocks(LockClient client, LockRequest request, BlockingMode blockingMode,
            @Nullable Long deadline, LockGroupBehavior lockGroupBehavior,
            Map<? super ClientAwareReadWriteLock, ? super LockMode> locks,
            Map<? super LockDescriptor, ? super LockClient> failedLocks)
            throws InterruptedException {
        String previousThreadName = null;
        try {
            previousThreadName = updateThreadName(request);
            for (Entry<LockDescriptor, LockMode> entry : request.getLockDescriptors().entries()) {
                if (blockingMode == BlockingMode.BLOCK_INDEFINITELY_THEN_RELEASE
                        && !descriptorToLockMap.asMap().containsKey(entry.getKey())) {
                    continue;
                }

                ClientAwareReadWriteLock lock;
                try {
                    lock = descriptorToLockMap.get(entry.getKey());
                } catch (ExecutionException e) {
                    throw new RuntimeException(e.getCause());
                }
                if (locks.containsKey(lock)) {
                    // This is the 2nd time we are calling tryLocks and we already locked this one.
                    continue;
                }
                long startTime = System.currentTimeMillis();
                @Nullable LockClient currentHolder = tryLock(lock.get(client, entry.getValue()),
                        blockingMode, deadline);
                if (log.isDebugEnabled()) {
                    long duration = System.currentTimeMillis() - startTime;
                    if (duration > 100) {
                        log.debug("Blocked for {} ms to acquire lock {} {}.",
                                duration,
                                entry.getKey().getLockIdAsString(),
                                currentHolder == null ? "successfully" : "unsuccessfully");
                    }
                }
                if (currentHolder == null) {
                    locks.put(lock, entry.getValue());
                } else {
                    failedLocks.put(entry.getKey(), currentHolder);
                    if (lockGroupBehavior == LOCK_ALL_OR_NONE) {
                        return;
                    }
                }
            }
        } finally {
            if (previousThreadName != null) {
                tryRenameThread(previousThreadName);
            }
        }
    }

    @Nullable private LockClient tryLock(KnownClientLock lock, BlockingMode blockingMode,
            @Nullable Long deadline) throws InterruptedException {
        switch (blockingMode) {
        case DO_NOT_BLOCK:
            return lock.tryLock();
        case BLOCK_UNTIL_TIMEOUT:
            return lock.tryLock(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
        case BLOCK_INDEFINITELY:
        case BLOCK_INDEFINITELY_THEN_RELEASE:
            lock.lockInterruptibly();
            return null;
        default:
            throw new IllegalArgumentException("blockingMode = " + blockingMode);
        }
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return unlockSimple(SimpleHeldLocksToken.fromLockRefreshToken(token));
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        Preconditions.checkNotNull(token);
        boolean success = unlockInternal(token, heldLocksTokenMap);
        if (log.isTraceEnabled()) {
            log.trace(".unlock(" + token + ") returns " + success);
        }
        return success;
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        Preconditions.checkNotNull(token);
        LockDescriptor fakeLockDesc = StringLockDescriptor.of("unlockSimple");
        SortedLockCollection<LockDescriptor> fakeLockSet = LockCollections.of(ImmutableSortedMap.of(fakeLockDesc, LockMode.READ));
        return unlock(new HeldLocksToken(
                token.getTokenId(),
                LockClient.ANONYMOUS,
                token.getCreationDateMs(),
                0L,
                fakeLockSet,
                maxAllowedLockTimeout,
                0L));
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        Preconditions.checkNotNull(token);
        @Nullable HeldLocks<HeldLocksToken> heldLocks = heldLocksTokenMap.remove(token);
        if (heldLocks == null) {
            if (log.isTraceEnabled()) {
                log.trace(".unlockAndFreeze(" + token + ") returns false");
            }
            return false;
        }
        LockClient client = heldLocks.realToken.getClient();
        if (client.isAnonymous()) {
            heldLocksTokenMap.put(token, heldLocks);
            lockTokenReaperQueue.add(token);
            String errorMessage =
                    "Received .unlockAndFreeze() call for anonymous client with token "
                    + heldLocks.realToken;
            log.warn(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
        if (heldLocks.locks.hasReadLock()) {
            heldLocksTokenMap.put(token, heldLocks);
            lockTokenReaperQueue.add(token);
            String errorMessage = "Received .unlockAndFreeze() call for read locks: "
                    + heldLocks.realToken;
            log.warn(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
        for (ClientAwareReadWriteLock lock : heldLocks.locks.getKeys()) {
            lock.get(client, LockMode.WRITE).unlockAndFreeze();
        }
        lockClientMultimap.remove(client, token);
        if (heldLocks.realToken.getVersionId() != null) {
            versionIdMap.remove(client, heldLocks.realToken.getVersionId());
        }
        if (log.isTraceEnabled()) {
            log.trace(".unlockAndFreeze(" + token + ") returns true");
        }
        return true;
    }

    private <T extends ExpiringToken> boolean unlockInternal(T token,
            ConcurrentMap<T, HeldLocks<T>> heldLocksMap) {
        @Nullable HeldLocks<T> heldLocks = heldLocksMap.remove(token);
        if (heldLocks == null) {
            return false;
        }

        long heldDuration = System.currentTimeMillis() - token.getCreationDateMs();
        if (requestLogger.isDebugEnabled()) {
            requestLogger.debug("Releasing locks {} after holding for {} ms",
                    heldLocks, heldDuration);
        }
        @Nullable LockClient client = heldLocks.realToken.getClient();
        if (client == null) {
            client = INTERNAL_LOCK_GRANT_CLIENT;
        } else {
            lockClientMultimap.remove(client, token);
        }
        for (Entry<? extends ClientAwareReadWriteLock, LockMode> entry : heldLocks.locks.entries()) {
            entry.getKey().get(client, entry.getValue()).unlock();
        }
        if (heldLocks.realToken.getVersionId() != null) {
            versionIdMap.remove(client, heldLocks.realToken.getVersionId());
        }
        return true;
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        Preconditions.checkNotNull(client);
        if (client.isAnonymous()) {
            throw new IllegalArgumentException("client must not be anonymous");
        } else if (client == INTERNAL_LOCK_GRANT_CLIENT) {
            throw new IllegalArgumentException("Illegal client!");
        }
        ImmutableSet.Builder<HeldLocksToken> tokens = ImmutableSet.builder();
        synchronized (lockClientMultimap) {
            for (HeldLocksToken token : lockClientMultimap.get(client)) {
                @Nullable HeldLocks<HeldLocksToken> heldLocks = heldLocksTokenMap.get(token);
                if ((heldLocks != null) && !isFrozen(heldLocks.locks.getKeys())) {
                    tokens.add(token);
                }
            }
        }
        ImmutableSet<HeldLocksToken> tokenSet = tokens.build();
        if (log.isTraceEnabled()) {
            log.trace(".getTokens(" + client + ") returns "
                    + Iterables.transform(tokenSet, TOKEN_TO_ID));
        }
        return tokenSet;
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        Preconditions.checkNotNull(tokens);
        ImmutableSet.Builder<HeldLocksToken> refreshedTokens = ImmutableSet.builder();
        for (HeldLocksToken token : tokens) {
            @Nullable HeldLocksToken refreshedToken = refreshToken(token);
            if (refreshedToken != null) {
                refreshedTokens.add(refreshedToken);
            }
        }
        Set<HeldLocksToken> refreshedTokenSet = refreshedTokens.build();
        if (log.isTraceEnabled()) {
            log.trace(".refreshTokens(" + Iterables.transform(tokens, TOKEN_TO_ID) + ") returns "
                    + Iterables.transform(refreshedTokenSet, TOKEN_TO_ID));
        }
        return refreshedTokenSet;
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        List<HeldLocksToken> fakeTokens = Lists.newArrayList();
        LockDescriptor fakeLockDesc = StringLockDescriptor.of("refreshLockRefreshTokens");
        SortedLockCollection<LockDescriptor> fakeLockSet = LockCollections.of(ImmutableSortedMap.of(fakeLockDesc, LockMode.READ));
        for (LockRefreshToken token : tokens) {
            fakeTokens.add(new HeldLocksToken(
                    token.getTokenId(),
                    LockClient.ANONYMOUS,
                    0L,
                    0L,
                    fakeLockSet,
                    maxAllowedLockTimeout,
                    0L));
        }
        return ImmutableSet.copyOf(Iterables.transform(refreshTokens(fakeTokens), HeldLocksTokens.getRefreshTokenFun()));
    }

    @Nullable private HeldLocksToken refreshToken(HeldLocksToken token) {
        Preconditions.checkNotNull(token);
        @Nullable HeldLocks<HeldLocksToken> heldLocks = heldLocksTokenMap.get(token);
        if ((heldLocks == null) || isFrozen(heldLocks.locks.getKeys())) {
            return null;
        }
        long now = currentTimeMillis();
        long expirationDateMs = now
                + heldLocks.realToken.getLockTimeout().toMillis();
        heldLocksTokenMap.replace(token, heldLocks, new HeldLocks<HeldLocksToken>(
                heldLocks.realToken.refresh(expirationDateMs), heldLocks.locks));
        heldLocks = heldLocksTokenMap.get(token);
        if (heldLocks == null) {
            return null;
        }
        HeldLocksToken finalToken = heldLocks.realToken;
        logIfAbnormallyOld(finalToken, now);
        return finalToken;
    }

    private boolean isFrozen(Iterable<? extends ClientAwareReadWriteLock> locks) {
        for (ClientAwareReadWriteLock lock : locks) {
            if (lock.isFrozen()) {
                return true;
            }
        }
        return false;
    }

    @Override
    @Nullable public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        Preconditions.checkNotNull(grant);
        @Nullable HeldLocks<HeldLocksGrant> heldLocks = heldLocksGrantMap.get(grant);
        if (heldLocks == null) {
            if (log.isTraceEnabled()) {
                log.trace(".refreshGrant(" + grant.getGrantId().toString(Character.MAX_RADIX)
                        + ") returns null");
            }
            return null;
        }
        long now = currentTimeMillis();
        long expirationDateMs = now
                + heldLocks.realToken.getLockTimeout().toMillis();
        heldLocksGrantMap.replace(grant, heldLocks, new HeldLocks<HeldLocksGrant>(
                heldLocks.realToken.refresh(expirationDateMs), heldLocks.locks));
        heldLocks = heldLocksGrantMap.get(grant);
        if (heldLocks == null) {
            if (log.isTraceEnabled()) {
                log.trace(".refreshGrant(" + grant.getGrantId().toString(Character.MAX_RADIX)
                        + ") returns null");
            }
            return null;
        }
        HeldLocksGrant refreshedGrant = heldLocks.realToken;
        logIfAbnormallyOld(refreshedGrant, now);
        if (log.isTraceEnabled()) {
            log.trace(".refreshGrant(" + grant.getGrantId().toString(Character.MAX_RADIX)
                    + ") returns " + refreshedGrant.getGrantId().toString(Character.MAX_RADIX));
        }
        return refreshedGrant;
    }

    private void logIfAbnormallyOld(final HeldLocksGrant grant, final long now) {
        logIfAbnormallyOld(grant, now,
                new Supplier<String>() {
                    @Override
                    public String get() {
                        return grant.toString(now);
                    }
                });
    }

    private void logIfAbnormallyOld(final HeldLocksToken token, final long now) {
        logIfAbnormallyOld(token, now,
                new Supplier<String>() {
                    @Override
                    public String get() {
                        return token.toString(now);
                    }
                });
    }

    private void logIfAbnormallyOld(ExpiringToken token, long now, Supplier<String> description) {
        if (log.isInfoEnabled()) {
            long age = now - token.getCreationDateMs();
            if (age > maxNormalLockAge.toMillis()) {
                log.debug("Token refreshed which is " + age + " ms old: " + description.get());
            }
        }
    }

    @Override
    @Nullable public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return refreshGrant(new HeldLocksGrant(Preconditions.checkNotNull(grantId)));
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        Preconditions.checkNotNull(token);
        @Nullable HeldLocks<HeldLocksToken> heldLocks = heldLocksTokenMap.remove(token);
        if (heldLocks == null) {
            log.warn("Cannot convert to grant; invalid token: " + token);
            throw new IllegalArgumentException("token is invalid: " + token);
        }
        if (isFrozen(heldLocks.locks.getKeys())) {
            heldLocksTokenMap.put(token, heldLocks);
            lockTokenReaperQueue.add(token);
            log.warn("Cannot convert to grant because token is frozen: " + token);
            throw new IllegalArgumentException("token is frozen: " + token);
        }
        try {
            changeOwner(heldLocks.locks, heldLocks.realToken.getClient(),
                    INTERNAL_LOCK_GRANT_CLIENT);
        } catch (IllegalMonitorStateException e) {
            heldLocksTokenMap.put(token, heldLocks);
            lockTokenReaperQueue.add(token);
            log.warn("Failure converting " + token + " to grant", e);
            throw e;
        }
        lockClientMultimap.remove(heldLocks.realToken.getClient(), token);
        HeldLocksGrant grant = createHeldLocksGrant(heldLocks.realToken.getLocks(),
                heldLocks.locks, heldLocks.realToken.getLockTimeout(),
                heldLocks.realToken.getVersionId());
        if (log.isTraceEnabled()) {
            log.trace(".convertToGrant(" + token + ") returns " + grant);
        }
        return grant;
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        Preconditions.checkNotNull(client);
        Preconditions.checkArgument(client != INTERNAL_LOCK_GRANT_CLIENT);
        Preconditions.checkNotNull(grant);
        @Nullable HeldLocks<HeldLocksGrant> heldLocks = heldLocksGrantMap.remove(grant);
        if (heldLocks == null) {
            log.warn("Tried to use invalid grant: " + grant);
            throw new IllegalArgumentException("grant is invalid: " + grant);
        }
        HeldLocksGrant realGrant = heldLocks.realToken;
        changeOwner(heldLocks.locks, INTERNAL_LOCK_GRANT_CLIENT, client);
        HeldLocksToken token = createHeldLocksToken(client, realGrant.getLocks(),
                heldLocks.locks, realGrant.getLockTimeout(), realGrant.getVersionId());
        if (log.isTraceEnabled()) {
            log.trace(".useGrant(" + client + ", " + grant + ") returns " + token);
        }
        return token;
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        Preconditions.checkNotNull(client);
        Preconditions.checkArgument(client != INTERNAL_LOCK_GRANT_CLIENT);
        Preconditions.checkNotNull(grantId);
        HeldLocksGrant grant = new HeldLocksGrant(grantId);
        @Nullable HeldLocks<HeldLocksGrant> heldLocks = heldLocksGrantMap.remove(grant);
        if (heldLocks == null) {
            String message = "Lock client " + client + " tried to use a lock grant that doesn't" +
                    " correspond to any held locks (grantId: " + grantId.toString(Character.MAX_RADIX) +
                    "); it's likely that this lock grant has expired due to timeout";
            log.warn(message);
            throw new IllegalArgumentException(message);
        }
        HeldLocksGrant realGrant = heldLocks.realToken;
        changeOwner(heldLocks.locks, INTERNAL_LOCK_GRANT_CLIENT, client);
        HeldLocksToken token = createHeldLocksToken(client, realGrant.getLocks(),
                heldLocks.locks, realGrant.getLockTimeout(), realGrant.getVersionId());
        if (log.isTraceEnabled()) {
            log.trace(".useGrant(" + client + ", " + grantId.toString(Character.MAX_RADIX)
                    + ") returns " + token);
        }
        return token;
    }

    private void changeOwner(LockCollection<? extends ClientAwareReadWriteLock> locks, LockClient oldClient,
            LockClient newClient) {
        Preconditions.checkArgument((oldClient == INTERNAL_LOCK_GRANT_CLIENT)
                != (newClient == INTERNAL_LOCK_GRANT_CLIENT));
        Collection<KnownClientLock> locksToRollback = Lists.newLinkedList();
        try {
            for (Entry<? extends ClientAwareReadWriteLock, LockMode> entry : locks.entries()) {
                ClientAwareReadWriteLock lock = entry.getKey();
                LockMode mode = entry.getValue();
                lock.get(oldClient, mode).changeOwner(newClient);
                locksToRollback.add(lock.get(newClient, mode));
            }
            locksToRollback.clear();
        } finally {
            /*
             * The above call to KnownLockClient.changeOwner() could throw an
             * IllegalMonitorStateException if we are CREATING a lock grant, but
             * it will never throw if we're changing the owner FROM the internal
             * lock grant client TO some named or anonymous client. This is true
             * because the internal lock grant client never increments the read
             * or write hold counts.
             *
             * In other words, if a lock has successfully made it into the grant
             * state (because the client didn't hold both the read lock and the
             * write lock, and didn't hold the write lock multiple times), then
             * these same conditions cannot possibly be violated between that
             * time and the time that the rollback operation below is performed.
             *
             * In conclusion, if this method is being called from
             * convertToGrant(), then the call to changeOwner() below will not
             * throw an exception, whereas if this method is being called from
             * useGrant(), then the try block above will not throw an exception,
             * which makes this finally block a no-op.
             */
            for (KnownClientLock lock : locksToRollback) {
                lock.changeOwner(oldClient);
            }
        }
    }

    @Override
    @Nullable public Long getMinLockedInVersionId() {
        return getMinLockedInVersionId(LockClient.ANONYMOUS);
    }

    @Override
    public Long getMinLockedInVersionId(String client) {
        return getMinLockedInVersionId(LockClient.of(client));
    }

    @Override
    @Nullable public Long getMinLockedInVersionId(LockClient client) {
        Long versionId = null;
        synchronized (versionIdMap) {
            Collection<Long> versionsForClient = versionIdMap.get(client);
            if (versionsForClient != null && !versionsForClient.isEmpty()) {
                versionId = versionsForClient.iterator().next();
            }
        }
        if (log.isTraceEnabled()) {
            log.trace(".getMinLockedInVersionId() returns " + versionId);
        }
        return versionId;
    }

    private <T extends ExpiringToken> void reapLocks(BlockingQueue<T> queue,
            ConcurrentMap<T, HeldLocks<T>> heldLocksMap) {
        while (true) {
            try {
                T token = null;
                try {
                    token = queue.take();
                    long sleepTimeMs = token.getExpirationDateMs() - currentTimeMillis()
                            + maxAllowedClockDrift.toMillis();
                    if (sleepTimeMs > 0) {
                        Thread.sleep(sleepTimeMs);
                    }
                } catch (InterruptedException e) {
                    if (isShutDown) {
                        break;
                    } else {
                        log.warn("The lock server reaper thread should not be " +
                                "interrupted if the server is not shutting down.", e);
                        if (token == null) {
                            continue;
                        }
                    }
                }
                @Nullable HeldLocks<T> heldLocks = heldLocksMap.get(token);
                if (heldLocks == null) {
                    continue;
                }
                T realToken = heldLocks.realToken;
                if (realToken.getExpirationDateMs() > currentTimeMillis()
                        - maxAllowedClockDrift.toMillis()) {
                    queue.add(realToken);
                } else {
                    log.warn("Lock token " + realToken
                            + " was not properly refreshed and is now being reaped.");
                    unlockInternal(realToken, heldLocksMap);
                }
            } catch (Throwable t) {
                log.error("Something went wrong while reaping locks. Attempting to continue anyway.", t);
            }
        }
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        LockServerOptions options = new LockServerOptions() {
            private static final long serialVersionUID = 0x3ffa5b160e838725l;
            @Override public boolean isStandaloneServer() {
                return isStandaloneServer;
            }
            @Override public TimeDuration getMaxAllowedLockTimeout() {
                return maxAllowedLockTimeout;
            }
            @Override public TimeDuration getMaxAllowedClockDrift() {
                return maxAllowedClockDrift;
            }
            @Override public TimeDuration getMaxAllowedBlockingDuration() {
                return maxAllowedBlockingDuration;
            }
            @Override public int getRandomBitCount() {
                return randomBitCount;
            }
        };
        if (log.isTraceEnabled()) {
            log.trace(".getLockServerOptions() returns " + options);
        }
        return options;
    }

    private <T> List<T> queueToOrderedList(Queue<T> queue) {
        List<T> list = Lists.newLinkedList();
        while (!queue.isEmpty()) {
            list.add(queue.poll());
        }
        for (T element : list) {
            queue.add(element);
        }
        return list;
    }

    /**
     * Prints the current state of the lock server to the logs. Useful for
     * debugging.
     */
    @Override
    public void logCurrentState() {
        StringBuilder logString = new StringBuilder();
        logString.append("Logging current state. Time = ").append(currentTimeMillis()).append("\n");
        logString.append("isStandaloneServer = ").append(isStandaloneServer).append("\n");
        logString.append("maxAllowedLockTimeout = ").append(maxAllowedLockTimeout).append("\n");
        logString.append("maxAllowedClockDrift = ").append(maxAllowedClockDrift).append("\n");
        logString.append("maxAllowedBlockingDuration = ").append(maxAllowedBlockingDuration).append("\n");
        logString.append("randomBitCount = ").append(randomBitCount).append("\n");
        for (Pair<String, ? extends Collection<?>> nameValuePair : ImmutableList.of(
                Pair.create("descriptorToLockMap", descriptorToLockMap.asMap().entrySet()),
                Pair.create("outstandingLockRequestMultimap", outstandingLockRequestMultimap.asMap().entrySet()),
                Pair.create("heldLocksTokenMap", heldLocksTokenMap.entrySet()),
                Pair.create("heldLocksGrantMap", heldLocksGrantMap.entrySet()),
                Pair.create("lockTokenReaperQueue", queueToOrderedList(lockTokenReaperQueue)),
                Pair.create("lockGrantReaperQueue", queueToOrderedList(lockGrantReaperQueue)),
                Pair.create("lockClientMultimap", lockClientMultimap.asMap().entrySet()),
                Pair.create("versionIdMap", versionIdMap.asMap().entrySet()))) {
            Collection<?> elements = nameValuePair.getRhSide();
            logString.append(nameValuePair.getLhSide()).append(".size() = ").append(elements.size()).append("\n");
            if (elements.size() > MAX_LOCKS_TO_LOG) {
                logString.append("WARNING: Only logging the first ").append(MAX_LOCKS_TO_LOG).append(" locks, ");
                logString.append("logging more is likely to OOM or slow down lock server to the point of failure");
            }
            for (Object element : Iterables.limit(elements, MAX_LOCKS_TO_LOG)) {
                logString.append(element).append("\n");
            }
        }
        logString.append("Finished logging current state. Time = ").append(currentTimeMillis());
        log.error(logString.toString());
    }

    @Override
    public void close() {
        isShutDown = true;
        executor.shutdownNow();
        callOnClose.run();
    }

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    private String getRequestDescription(LockRequest request) {
        StringBuilder builder = new StringBuilder();
        builder.append("\twaiting to lock() ").append(request);
        builder.append("\n\tfor requesting thread\n\t\t").append(request.getCreatingThreadName()).append("\n");
        return builder.toString();
    }

    private String updateThreadName(LockRequest request) {
        String currentThreadName = Thread.currentThread().getName();
        String requestDescription = getRequestDescription(request);
        tryRenameThread(currentThreadName + "\n" + requestDescription);
        return currentThreadName;
    }

    private void tryRenameThread(String name) {
        try {
            Thread.currentThread().setName(name);
        } catch (SecurityException ex) {
            requestLogger.error("Cannot rename LockServer threads");
        }
    }

}
