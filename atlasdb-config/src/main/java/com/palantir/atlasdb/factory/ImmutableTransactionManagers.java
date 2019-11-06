/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import org.immutables.value.Generated;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.lock.LockServerOptions;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Immutable implementation of {@link TransactionManagers}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTransactionManagers.builder()}.
 */
@Generated(from = "TransactionManagers", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableTransactionManagers extends TransactionManagers {
  private final AtlasDbConfig config;
  private final Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier;
  private final ImmutableSet<Schema> schemas;
  private final Consumer<Object> registrar;
  private final LockServerOptions lockServerOptions;
  private final boolean allowHiddenTableAccess;
  private final boolean validateLocksOnReads;
  private final boolean lockImmutableTsOnReadOnlyTransactions;
  private final boolean allSafeForLogging;
  private final UserAgent userAgent;
  private final MetricRegistry globalMetricsRegistry;
  private final TaggedMetricRegistry globalTaggedMetricRegistry;
  private final Callback<TransactionManager> asyncInitializationCallback;
  private transient final TransactionManager serializable;

  private ImmutableTransactionManagers(Builder builder) {
    this.config = builder.config;
    this.schemas = builder.schemas.build();
    this.userAgent = builder.userAgent;
    this.globalMetricsRegistry = builder.globalMetricsRegistry;
    this.globalTaggedMetricRegistry = builder.globalTaggedMetricRegistry;
    if (builder.runtimeConfigSupplierIsSet()) {
      initShim.runtimeConfigSupplier(builder.runtimeConfigSupplier);
    }
    if (builder.registrarIsSet()) {
      initShim.registrar(builder.registrar);
    }
    if (builder.lockServerOptionsIsSet()) {
      initShim.lockServerOptions(builder.lockServerOptions);
    }
    if (builder.allowHiddenTableAccessIsSet()) {
      initShim.allowHiddenTableAccess(builder.allowHiddenTableAccess);
    }
    if (builder.validateLocksOnReadsIsSet()) {
      initShim.validateLocksOnReads(builder.validateLocksOnReads);
    }
    if (builder.lockImmutableTsOnReadOnlyTransactionsIsSet()) {
      initShim.lockImmutableTsOnReadOnlyTransactions(builder.lockImmutableTsOnReadOnlyTransactions);
    }
    if (builder.allSafeForLoggingIsSet()) {
      initShim.allSafeForLogging(builder.allSafeForLogging);
    }
    if (builder.asyncInitializationCallbackIsSet()) {
      initShim.asyncInitializationCallback(builder.asyncInitializationCallback);
    }
    this.runtimeConfigSupplier = initShim.runtimeConfigSupplier();
    this.registrar = initShim.registrar();
    this.lockServerOptions = initShim.lockServerOptions();
    this.allowHiddenTableAccess = initShim.allowHiddenTableAccess();
    this.validateLocksOnReads = initShim.validateLocksOnReads();
    this.lockImmutableTsOnReadOnlyTransactions = initShim.lockImmutableTsOnReadOnlyTransactions();
    this.allSafeForLogging = initShim.allSafeForLogging();
    this.asyncInitializationCallback = initShim.asyncInitializationCallback();
    this.serializable = initShim.serializable();
    this.initShim = null;
  }

  private ImmutableTransactionManagers(
      AtlasDbConfig config,
      Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
      ImmutableSet<Schema> schemas,
      Consumer<Object> registrar,
      LockServerOptions lockServerOptions,
      boolean allowHiddenTableAccess,
      boolean validateLocksOnReads,
      boolean lockImmutableTsOnReadOnlyTransactions,
      boolean allSafeForLogging,
      UserAgent userAgent,
      MetricRegistry globalMetricsRegistry,
      TaggedMetricRegistry globalTaggedMetricRegistry,
      Callback<TransactionManager> asyncInitializationCallback) {
    this.config = config;
    initShim.runtimeConfigSupplier(runtimeConfigSupplier);
    this.schemas = schemas;
    initShim.registrar(registrar);
    initShim.lockServerOptions(lockServerOptions);
    initShim.allowHiddenTableAccess(allowHiddenTableAccess);
    initShim.validateLocksOnReads(validateLocksOnReads);
    initShim.lockImmutableTsOnReadOnlyTransactions(lockImmutableTsOnReadOnlyTransactions);
    initShim.allSafeForLogging(allSafeForLogging);
    this.userAgent = userAgent;
    this.globalMetricsRegistry = globalMetricsRegistry;
    this.globalTaggedMetricRegistry = globalTaggedMetricRegistry;
    initShim.asyncInitializationCallback(asyncInitializationCallback);
    this.runtimeConfigSupplier = initShim.runtimeConfigSupplier();
    this.registrar = initShim.registrar();
    this.lockServerOptions = initShim.lockServerOptions();
    this.allowHiddenTableAccess = initShim.allowHiddenTableAccess();
    this.validateLocksOnReads = initShim.validateLocksOnReads();
    this.lockImmutableTsOnReadOnlyTransactions = initShim.lockImmutableTsOnReadOnlyTransactions();
    this.allSafeForLogging = initShim.allSafeForLogging();
    this.asyncInitializationCallback = initShim.asyncInitializationCallback();
    this.serializable = initShim.serializable();
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "TransactionManagers", generator = "Immutables")
  private final class InitShim {
    private byte runtimeConfigSupplierBuildStage = STAGE_UNINITIALIZED;
    private Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier;

    Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier() {
      if (runtimeConfigSupplierBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (runtimeConfigSupplierBuildStage == STAGE_UNINITIALIZED) {
        runtimeConfigSupplierBuildStage = STAGE_INITIALIZING;
        this.runtimeConfigSupplier = Objects.requireNonNull(ImmutableTransactionManagers.super.runtimeConfigSupplier(), "runtimeConfigSupplier");
        runtimeConfigSupplierBuildStage = STAGE_INITIALIZED;
      }
      return this.runtimeConfigSupplier;
    }

    void runtimeConfigSupplier(Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier) {
      this.runtimeConfigSupplier = runtimeConfigSupplier;
      runtimeConfigSupplierBuildStage = STAGE_INITIALIZED;
    }

    private byte registrarBuildStage = STAGE_UNINITIALIZED;
    private Consumer<Object> registrar;

    Consumer<Object> registrar() {
      if (registrarBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (registrarBuildStage == STAGE_UNINITIALIZED) {
        registrarBuildStage = STAGE_INITIALIZING;
        this.registrar = Objects.requireNonNull(ImmutableTransactionManagers.super.registrar(), "registrar");
        registrarBuildStage = STAGE_INITIALIZED;
      }
      return this.registrar;
    }

    void registrar(Consumer<Object> registrar) {
      this.registrar = registrar;
      registrarBuildStage = STAGE_INITIALIZED;
    }

    private byte lockServerOptionsBuildStage = STAGE_UNINITIALIZED;
    private LockServerOptions lockServerOptions;

    LockServerOptions lockServerOptions() {
      if (lockServerOptionsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (lockServerOptionsBuildStage == STAGE_UNINITIALIZED) {
        lockServerOptionsBuildStage = STAGE_INITIALIZING;
        this.lockServerOptions = Objects.requireNonNull(ImmutableTransactionManagers.super.lockServerOptions(), "lockServerOptions");
        lockServerOptionsBuildStage = STAGE_INITIALIZED;
      }
      return this.lockServerOptions;
    }

    void lockServerOptions(LockServerOptions lockServerOptions) {
      this.lockServerOptions = lockServerOptions;
      lockServerOptionsBuildStage = STAGE_INITIALIZED;
    }

    private byte allowHiddenTableAccessBuildStage = STAGE_UNINITIALIZED;
    private boolean allowHiddenTableAccess;

    boolean allowHiddenTableAccess() {
      if (allowHiddenTableAccessBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowHiddenTableAccessBuildStage == STAGE_UNINITIALIZED) {
        allowHiddenTableAccessBuildStage = STAGE_INITIALIZING;
        this.allowHiddenTableAccess = ImmutableTransactionManagers.super.allowHiddenTableAccess();
        allowHiddenTableAccessBuildStage = STAGE_INITIALIZED;
      }
      return this.allowHiddenTableAccess;
    }

    void allowHiddenTableAccess(boolean allowHiddenTableAccess) {
      this.allowHiddenTableAccess = allowHiddenTableAccess;
      allowHiddenTableAccessBuildStage = STAGE_INITIALIZED;
    }

    private byte validateLocksOnReadsBuildStage = STAGE_UNINITIALIZED;
    private boolean validateLocksOnReads;

    boolean validateLocksOnReads() {
      if (validateLocksOnReadsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (validateLocksOnReadsBuildStage == STAGE_UNINITIALIZED) {
        validateLocksOnReadsBuildStage = STAGE_INITIALIZING;
        this.validateLocksOnReads = ImmutableTransactionManagers.super.validateLocksOnReads();
        validateLocksOnReadsBuildStage = STAGE_INITIALIZED;
      }
      return this.validateLocksOnReads;
    }

    void validateLocksOnReads(boolean validateLocksOnReads) {
      this.validateLocksOnReads = validateLocksOnReads;
      validateLocksOnReadsBuildStage = STAGE_INITIALIZED;
    }

    private byte lockImmutableTsOnReadOnlyTransactionsBuildStage = STAGE_UNINITIALIZED;
    private boolean lockImmutableTsOnReadOnlyTransactions;

    boolean lockImmutableTsOnReadOnlyTransactions() {
      if (lockImmutableTsOnReadOnlyTransactionsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (lockImmutableTsOnReadOnlyTransactionsBuildStage == STAGE_UNINITIALIZED) {
        lockImmutableTsOnReadOnlyTransactionsBuildStage = STAGE_INITIALIZING;
        this.lockImmutableTsOnReadOnlyTransactions = ImmutableTransactionManagers.super.lockImmutableTsOnReadOnlyTransactions();
        lockImmutableTsOnReadOnlyTransactionsBuildStage = STAGE_INITIALIZED;
      }
      return this.lockImmutableTsOnReadOnlyTransactions;
    }

    void lockImmutableTsOnReadOnlyTransactions(boolean lockImmutableTsOnReadOnlyTransactions) {
      this.lockImmutableTsOnReadOnlyTransactions = lockImmutableTsOnReadOnlyTransactions;
      lockImmutableTsOnReadOnlyTransactionsBuildStage = STAGE_INITIALIZED;
    }

    private byte allSafeForLoggingBuildStage = STAGE_UNINITIALIZED;
    private boolean allSafeForLogging;

    boolean allSafeForLogging() {
      if (allSafeForLoggingBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allSafeForLoggingBuildStage == STAGE_UNINITIALIZED) {
        allSafeForLoggingBuildStage = STAGE_INITIALIZING;
        this.allSafeForLogging = ImmutableTransactionManagers.super.allSafeForLogging();
        allSafeForLoggingBuildStage = STAGE_INITIALIZED;
      }
      return this.allSafeForLogging;
    }

    void allSafeForLogging(boolean allSafeForLogging) {
      this.allSafeForLogging = allSafeForLogging;
      allSafeForLoggingBuildStage = STAGE_INITIALIZED;
    }

    private byte asyncInitializationCallbackBuildStage = STAGE_UNINITIALIZED;
    private Callback<TransactionManager> asyncInitializationCallback;

    Callback<TransactionManager> asyncInitializationCallback() {
      if (asyncInitializationCallbackBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (asyncInitializationCallbackBuildStage == STAGE_UNINITIALIZED) {
        asyncInitializationCallbackBuildStage = STAGE_INITIALIZING;
        this.asyncInitializationCallback = Objects.requireNonNull(ImmutableTransactionManagers.super.asyncInitializationCallback(), "asyncInitializationCallback");
        asyncInitializationCallbackBuildStage = STAGE_INITIALIZED;
      }
      return this.asyncInitializationCallback;
    }

    void asyncInitializationCallback(Callback<TransactionManager> asyncInitializationCallback) {
      this.asyncInitializationCallback = asyncInitializationCallback;
      asyncInitializationCallbackBuildStage = STAGE_INITIALIZED;
    }

    private byte serializableBuildStage = STAGE_UNINITIALIZED;
    private TransactionManager serializable;

    TransactionManager serializable() {
      if (serializableBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (serializableBuildStage == STAGE_UNINITIALIZED) {
        serializableBuildStage = STAGE_INITIALIZING;
        this.serializable = Objects.requireNonNull(ImmutableTransactionManagers.super.serializable(), "serializable");
        serializableBuildStage = STAGE_INITIALIZED;
      }
      return this.serializable;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (runtimeConfigSupplierBuildStage == STAGE_INITIALIZING) attributes.add("runtimeConfigSupplier");
      if (registrarBuildStage == STAGE_INITIALIZING) attributes.add("registrar");
      if (lockServerOptionsBuildStage == STAGE_INITIALIZING) attributes.add("lockServerOptions");
      if (allowHiddenTableAccessBuildStage == STAGE_INITIALIZING) attributes.add("allowHiddenTableAccess");
      if (validateLocksOnReadsBuildStage == STAGE_INITIALIZING) attributes.add("validateLocksOnReads");
      if (lockImmutableTsOnReadOnlyTransactionsBuildStage == STAGE_INITIALIZING) attributes.add("lockImmutableTsOnReadOnlyTransactions");
      if (allSafeForLoggingBuildStage == STAGE_INITIALIZING) attributes.add("allSafeForLogging");
      if (asyncInitializationCallbackBuildStage == STAGE_INITIALIZING) attributes.add("asyncInitializationCallback");
      if (serializableBuildStage == STAGE_INITIALIZING) attributes.add("serializable");
      return "Cannot build TransactionManagers, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The value of the {@code config} attribute
   */
  @Override
  AtlasDbConfig config() {
    return config;
  }

  /**
   * @return The value of the {@code runtimeConfigSupplier} attribute
   */
  @Override
  Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.runtimeConfigSupplier()
        : this.runtimeConfigSupplier;
  }

  /**
   * @return The value of the {@code schemas} attribute
   */
  @Override
  ImmutableSet<Schema> schemas() {
    return schemas;
  }

  /**
   * @return The value of the {@code registrar} attribute
   */
  @Override
  Consumer<Object> registrar() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.registrar()
        : this.registrar;
  }

  /**
   * @return The value of the {@code lockServerOptions} attribute
   */
  @Override
  LockServerOptions lockServerOptions() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.lockServerOptions()
        : this.lockServerOptions;
  }

  /**
   * @return The value of the {@code allowHiddenTableAccess} attribute
   */
  @Override
  boolean allowHiddenTableAccess() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.allowHiddenTableAccess()
        : this.allowHiddenTableAccess;
  }

  /**
   * @return The value of the {@code validateLocksOnReads} attribute
   */
  @Override
  boolean validateLocksOnReads() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.validateLocksOnReads()
        : this.validateLocksOnReads;
  }

  /**
   * @return The value of the {@code lockImmutableTsOnReadOnlyTransactions} attribute
   */
  @Override
  boolean lockImmutableTsOnReadOnlyTransactions() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.lockImmutableTsOnReadOnlyTransactions()
        : this.lockImmutableTsOnReadOnlyTransactions;
  }

  /**
   * @return The value of the {@code allSafeForLogging} attribute
   */
  @Override
  boolean allSafeForLogging() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.allSafeForLogging()
        : this.allSafeForLogging;
  }

  /**
   * @return The value of the {@code userAgent} attribute
   */
  @Override
  UserAgent userAgent() {
    return userAgent;
  }

  /**
   * @return The value of the {@code globalMetricsRegistry} attribute
   */
  @Override
  MetricRegistry globalMetricsRegistry() {
    return globalMetricsRegistry;
  }

  /**
   * @return The value of the {@code globalTaggedMetricRegistry} attribute
   */
  @Override
  TaggedMetricRegistry globalTaggedMetricRegistry() {
    return globalTaggedMetricRegistry;
  }

  /**
   * The callback Runnable will be run when the TransactionManager is successfully initialized. The
   * TransactionManager will stay uninitialized and continue to throw for all other purposes until the callback
   * returns at which point it will become initialized. If asynchronous initialization is disabled, the callback will
   * be run just before the TM is returned.
   * Note that if the callback blocks forever, the TransactionManager will never become initialized, and calling its
   * close() method will block forever as well. If the callback init() fails, and its cleanup() method throws,
   * the TransactionManager will not become initialized and it will be closed.
   */
  @Override
  Callback<TransactionManager> asyncInitializationCallback() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.asyncInitializationCallback()
        : this.asyncInitializationCallback;
  }

  /**
   * @return The computed-at-construction value of the {@code serializable} attribute
   */
  @Override
  public TransactionManager serializable() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.serializable()
        : this.serializable;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#config() config} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for config
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withConfig(AtlasDbConfig value) {
    if (this.config == value) return this;
    AtlasDbConfig newValue = Objects.requireNonNull(value, "config");
    return new ImmutableTransactionManagers(
        newValue,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#runtimeConfigSupplier() runtimeConfigSupplier} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for runtimeConfigSupplier
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withRuntimeConfigSupplier(Supplier<Optional<AtlasDbRuntimeConfig>> value) {
    if (this.runtimeConfigSupplier == value) return this;
    Supplier<Optional<AtlasDbRuntimeConfig>> newValue = Objects.requireNonNull(value, "runtimeConfigSupplier");
    return new ImmutableTransactionManagers(
        this.config,
        newValue,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TransactionManagers#schemas() schemas}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTransactionManagers withSchemas(Schema... elements) {
    ImmutableSet<Schema> newValue = ImmutableSet.copyOf(elements);
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        newValue,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TransactionManagers#schemas() schemas}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of schemas elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTransactionManagers withSchemas(Iterable<? extends Schema> elements) {
    if (this.schemas == elements) return this;
    ImmutableSet<Schema> newValue = ImmutableSet.copyOf(elements);
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        newValue,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#registrar() registrar} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for registrar
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withRegistrar(Consumer<Object> value) {
    if (this.registrar == value) return this;
    Consumer<Object> newValue = Objects.requireNonNull(value, "registrar");
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        newValue,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#lockServerOptions() lockServerOptions} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for lockServerOptions
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withLockServerOptions(LockServerOptions value) {
    if (this.lockServerOptions == value) return this;
    LockServerOptions newValue = Objects.requireNonNull(value, "lockServerOptions");
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        newValue,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#allowHiddenTableAccess() allowHiddenTableAccess} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowHiddenTableAccess
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withAllowHiddenTableAccess(boolean value) {
    if (this.allowHiddenTableAccess == value) return this;
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        value,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#validateLocksOnReads() validateLocksOnReads} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for validateLocksOnReads
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withValidateLocksOnReads(boolean value) {
    if (this.validateLocksOnReads == value) return this;
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        value,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#lockImmutableTsOnReadOnlyTransactions() lockImmutableTsOnReadOnlyTransactions} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for lockImmutableTsOnReadOnlyTransactions
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withLockImmutableTsOnReadOnlyTransactions(boolean value) {
    if (this.lockImmutableTsOnReadOnlyTransactions == value) return this;
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        value,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#allSafeForLogging() allSafeForLogging} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allSafeForLogging
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withAllSafeForLogging(boolean value) {
    if (this.allSafeForLogging == value) return this;
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        value,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#userAgent() userAgent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for userAgent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withUserAgent(UserAgent value) {
    if (this.userAgent == value) return this;
    UserAgent newValue = Objects.requireNonNull(value, "userAgent");
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        newValue,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#globalMetricsRegistry() globalMetricsRegistry} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for globalMetricsRegistry
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withGlobalMetricsRegistry(MetricRegistry value) {
    if (this.globalMetricsRegistry == value) return this;
    MetricRegistry newValue = Objects.requireNonNull(value, "globalMetricsRegistry");
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        newValue,
        this.globalTaggedMetricRegistry,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#globalTaggedMetricRegistry() globalTaggedMetricRegistry} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for globalTaggedMetricRegistry
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withGlobalTaggedMetricRegistry(TaggedMetricRegistry value) {
    if (this.globalTaggedMetricRegistry == value) return this;
    TaggedMetricRegistry newValue = Objects.requireNonNull(value, "globalTaggedMetricRegistry");
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        newValue,
        this.asyncInitializationCallback);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TransactionManagers#asyncInitializationCallback() asyncInitializationCallback} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for asyncInitializationCallback
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTransactionManagers withAsyncInitializationCallback(Callback<TransactionManager> value) {
    if (this.asyncInitializationCallback == value) return this;
    Callback<TransactionManager> newValue = Objects.requireNonNull(value, "asyncInitializationCallback");
    return new ImmutableTransactionManagers(
        this.config,
        this.runtimeConfigSupplier,
        this.schemas,
        this.registrar,
        this.lockServerOptions,
        this.allowHiddenTableAccess,
        this.validateLocksOnReads,
        this.lockImmutableTsOnReadOnlyTransactions,
        this.allSafeForLogging,
        this.userAgent,
        this.globalMetricsRegistry,
        this.globalTaggedMetricRegistry,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTransactionManagers} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTransactionManagers
        && equalTo((ImmutableTransactionManagers) another);
  }

  private boolean equalTo(ImmutableTransactionManagers another) {
    return config.equals(another.config)
        && runtimeConfigSupplier.equals(another.runtimeConfigSupplier)
        && schemas.equals(another.schemas)
        && registrar.equals(another.registrar)
        && lockServerOptions.equals(another.lockServerOptions)
        && allowHiddenTableAccess == another.allowHiddenTableAccess
        && validateLocksOnReads == another.validateLocksOnReads
        && lockImmutableTsOnReadOnlyTransactions == another.lockImmutableTsOnReadOnlyTransactions
        && allSafeForLogging == another.allSafeForLogging
        && userAgent.equals(another.userAgent)
        && globalMetricsRegistry.equals(another.globalMetricsRegistry)
        && globalTaggedMetricRegistry.equals(another.globalTaggedMetricRegistry)
        && asyncInitializationCallback.equals(another.asyncInitializationCallback)
        && serializable.equals(another.serializable);
  }

  /**
   * Computes a hash code from attributes: {@code config}, {@code runtimeConfigSupplier}, {@code schemas}, {@code registrar}, {@code lockServerOptions}, {@code allowHiddenTableAccess}, {@code validateLocksOnReads}, {@code lockImmutableTsOnReadOnlyTransactions}, {@code allSafeForLogging}, {@code userAgent}, {@code globalMetricsRegistry}, {@code globalTaggedMetricRegistry}, {@code asyncInitializationCallback}, {@code serializable}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + config.hashCode();
    h += (h << 5) + runtimeConfigSupplier.hashCode();
    h += (h << 5) + schemas.hashCode();
    h += (h << 5) + registrar.hashCode();
    h += (h << 5) + lockServerOptions.hashCode();
    h += (h << 5) + Booleans.hashCode(allowHiddenTableAccess);
    h += (h << 5) + Booleans.hashCode(validateLocksOnReads);
    h += (h << 5) + Booleans.hashCode(lockImmutableTsOnReadOnlyTransactions);
    h += (h << 5) + Booleans.hashCode(allSafeForLogging);
    h += (h << 5) + userAgent.hashCode();
    h += (h << 5) + globalMetricsRegistry.hashCode();
    h += (h << 5) + globalTaggedMetricRegistry.hashCode();
    h += (h << 5) + asyncInitializationCallback.hashCode();
    h += (h << 5) + serializable.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TransactionManagers} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("TransactionManagers")
        .omitNullValues()
        .add("config", config)
        .add("runtimeConfigSupplier", runtimeConfigSupplier)
        .add("schemas", schemas)
        .add("registrar", registrar)
        .add("lockServerOptions", lockServerOptions)
        .add("allowHiddenTableAccess", allowHiddenTableAccess)
        .add("validateLocksOnReads", validateLocksOnReads)
        .add("lockImmutableTsOnReadOnlyTransactions", lockImmutableTsOnReadOnlyTransactions)
        .add("allSafeForLogging", allSafeForLogging)
        .add("userAgent", userAgent)
        .add("globalMetricsRegistry", globalMetricsRegistry)
        .add("globalTaggedMetricRegistry", globalTaggedMetricRegistry)
        .add("asyncInitializationCallback", asyncInitializationCallback)
        .add("serializable", serializable)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link TransactionManagers} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TransactionManagers instance
   */
  public static ImmutableTransactionManagers copyOf(TransactionManagers instance) {
    if (instance instanceof ImmutableTransactionManagers) {
      return (ImmutableTransactionManagers) instance;
    }
    return ((Builder) ImmutableTransactionManagers.builder())
        .config(instance.config())
        .runtimeConfigSupplier(instance.runtimeConfigSupplier())
        .addAllSchemas(instance.schemas())
        .registrar(instance.registrar())
        .lockServerOptions(instance.lockServerOptions())
        .allowHiddenTableAccess(instance.allowHiddenTableAccess())
        .validateLocksOnReads(instance.validateLocksOnReads())
        .lockImmutableTsOnReadOnlyTransactions(instance.lockImmutableTsOnReadOnlyTransactions())
        .allSafeForLogging(instance.allSafeForLogging())
        .userAgent(instance.userAgent())
        .globalMetricsRegistry(instance.globalMetricsRegistry())
        .globalTaggedMetricRegistry(instance.globalTaggedMetricRegistry())
        .asyncInitializationCallback(instance.asyncInitializationCallback())
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTransactionManagers ImmutableTransactionManagers}.
   * @return A new ImmutableTransactionManagers builder
   */
  public static ConfigBuildStage builder() {
    return new Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTransactionManagers ImmutableTransactionManagers}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TransactionManagers", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder
      implements ConfigBuildStage, UserAgentBuildStage, GlobalMetricsRegistryBuildStage, GlobalTaggedMetricRegistryBuildStage, BuildFinal {
    private static final long INIT_BIT_CONFIG = 0x1L;
    private static final long INIT_BIT_USER_AGENT = 0x2L;
    private static final long INIT_BIT_GLOBAL_METRICS_REGISTRY = 0x4L;
    private static final long INIT_BIT_GLOBAL_TAGGED_METRIC_REGISTRY = 0x8L;
    private static final long OPT_BIT_RUNTIME_CONFIG_SUPPLIER = 0x1L;
    private static final long OPT_BIT_REGISTRAR = 0x2L;
    private static final long OPT_BIT_LOCK_SERVER_OPTIONS = 0x4L;
    private static final long OPT_BIT_ALLOW_HIDDEN_TABLE_ACCESS = 0x8L;
    private static final long OPT_BIT_VALIDATE_LOCKS_ON_READS = 0x10L;
    private static final long OPT_BIT_LOCK_IMMUTABLE_TS_ON_READ_ONLY_TRANSACTIONS = 0x20L;
    private static final long OPT_BIT_ALL_SAFE_FOR_LOGGING = 0x40L;
    private static final long OPT_BIT_ASYNC_INITIALIZATION_CALLBACK = 0x80L;
    private long initBits = 0xfL;
    private long optBits;

    private @Nullable AtlasDbConfig config;
    private @Nullable Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier;
    private final ImmutableSet.Builder<Schema> schemas = ImmutableSet.builder();
    private @Nullable Consumer<Object> registrar;
    private @Nullable LockServerOptions lockServerOptions;
    private boolean allowHiddenTableAccess;
    private boolean validateLocksOnReads;
    private boolean lockImmutableTsOnReadOnlyTransactions;
    private boolean allSafeForLogging;
    private @Nullable UserAgent userAgent;
    private @Nullable MetricRegistry globalMetricsRegistry;
    private @Nullable TaggedMetricRegistry globalTaggedMetricRegistry;
    private @Nullable Callback<TransactionManager> asyncInitializationCallback;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link TransactionManagers#config() config} attribute.
     * @param config The value for config
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder config(AtlasDbConfig config) {
      checkNotIsSet(configIsSet(), "config");
      this.config = Objects.requireNonNull(config, "config");
      initBits &= ~INIT_BIT_CONFIG;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#runtimeConfigSupplier() runtimeConfigSupplier} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#runtimeConfigSupplier() runtimeConfigSupplier}.</em>
     * @param runtimeConfigSupplier The value for runtimeConfigSupplier
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder runtimeConfigSupplier(Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier) {
      checkNotIsSet(runtimeConfigSupplierIsSet(), "runtimeConfigSupplier");
      this.runtimeConfigSupplier = Objects.requireNonNull(runtimeConfigSupplier, "runtimeConfigSupplier");
      optBits |= OPT_BIT_RUNTIME_CONFIG_SUPPLIER;
      return this;
    }

    /**
     * Adds one element to {@link TransactionManagers#schemas() schemas} set.
     * @param element A schemas element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder addSchemas(Schema element) {
      this.schemas.add(element);
      return this;
    }

    /**
     * Adds elements to {@link TransactionManagers#schemas() schemas} set.
     * @param elements An array of schemas elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder addSchemas(Schema... elements) {
      this.schemas.add(elements);
      return this;
    }


    /**
     * Adds elements to {@link TransactionManagers#schemas() schemas} set.
     * @param elements An iterable of schemas elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder addAllSchemas(Iterable<? extends Schema> elements) {
      this.schemas.addAll(elements);
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#registrar() registrar} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#registrar() registrar}.</em>
     * @param registrar The value for registrar
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder registrar(Consumer<Object> registrar) {
      checkNotIsSet(registrarIsSet(), "registrar");
      this.registrar = Objects.requireNonNull(registrar, "registrar");
      optBits |= OPT_BIT_REGISTRAR;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#lockServerOptions() lockServerOptions} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#lockServerOptions() lockServerOptions}.</em>
     * @param lockServerOptions The value for lockServerOptions
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder lockServerOptions(LockServerOptions lockServerOptions) {
      checkNotIsSet(lockServerOptionsIsSet(), "lockServerOptions");
      this.lockServerOptions = Objects.requireNonNull(lockServerOptions, "lockServerOptions");
      optBits |= OPT_BIT_LOCK_SERVER_OPTIONS;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#allowHiddenTableAccess() allowHiddenTableAccess} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#allowHiddenTableAccess() allowHiddenTableAccess}.</em>
     * @param allowHiddenTableAccess The value for allowHiddenTableAccess
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder allowHiddenTableAccess(boolean allowHiddenTableAccess) {
      checkNotIsSet(allowHiddenTableAccessIsSet(), "allowHiddenTableAccess");
      this.allowHiddenTableAccess = allowHiddenTableAccess;
      optBits |= OPT_BIT_ALLOW_HIDDEN_TABLE_ACCESS;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#validateLocksOnReads() validateLocksOnReads} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#validateLocksOnReads() validateLocksOnReads}.</em>
     * @param validateLocksOnReads The value for validateLocksOnReads
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder validateLocksOnReads(boolean validateLocksOnReads) {
      checkNotIsSet(validateLocksOnReadsIsSet(), "validateLocksOnReads");
      this.validateLocksOnReads = validateLocksOnReads;
      optBits |= OPT_BIT_VALIDATE_LOCKS_ON_READS;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#lockImmutableTsOnReadOnlyTransactions() lockImmutableTsOnReadOnlyTransactions} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#lockImmutableTsOnReadOnlyTransactions() lockImmutableTsOnReadOnlyTransactions}.</em>
     * @param lockImmutableTsOnReadOnlyTransactions The value for lockImmutableTsOnReadOnlyTransactions
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder lockImmutableTsOnReadOnlyTransactions(boolean lockImmutableTsOnReadOnlyTransactions) {
      checkNotIsSet(lockImmutableTsOnReadOnlyTransactionsIsSet(), "lockImmutableTsOnReadOnlyTransactions");
      this.lockImmutableTsOnReadOnlyTransactions = lockImmutableTsOnReadOnlyTransactions;
      optBits |= OPT_BIT_LOCK_IMMUTABLE_TS_ON_READ_ONLY_TRANSACTIONS;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#allSafeForLogging() allSafeForLogging} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#allSafeForLogging() allSafeForLogging}.</em>
     * @param allSafeForLogging The value for allSafeForLogging
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder allSafeForLogging(boolean allSafeForLogging) {
      checkNotIsSet(allSafeForLoggingIsSet(), "allSafeForLogging");
      this.allSafeForLogging = allSafeForLogging;
      optBits |= OPT_BIT_ALL_SAFE_FOR_LOGGING;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#userAgent() userAgent} attribute.
     * @param userAgent The value for userAgent
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder userAgent(UserAgent userAgent) {
      checkNotIsSet(userAgentIsSet(), "userAgent");
      this.userAgent = Objects.requireNonNull(userAgent, "userAgent");
      initBits &= ~INIT_BIT_USER_AGENT;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#globalMetricsRegistry() globalMetricsRegistry} attribute.
     * @param globalMetricsRegistry The value for globalMetricsRegistry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder globalMetricsRegistry(MetricRegistry globalMetricsRegistry) {
      checkNotIsSet(globalMetricsRegistryIsSet(), "globalMetricsRegistry");
      this.globalMetricsRegistry = Objects.requireNonNull(globalMetricsRegistry, "globalMetricsRegistry");
      initBits &= ~INIT_BIT_GLOBAL_METRICS_REGISTRY;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#globalTaggedMetricRegistry() globalTaggedMetricRegistry} attribute.
     * @param globalTaggedMetricRegistry The value for globalTaggedMetricRegistry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder globalTaggedMetricRegistry(TaggedMetricRegistry globalTaggedMetricRegistry) {
      checkNotIsSet(globalTaggedMetricRegistryIsSet(), "globalTaggedMetricRegistry");
      this.globalTaggedMetricRegistry = Objects.requireNonNull(globalTaggedMetricRegistry, "globalTaggedMetricRegistry");
      initBits &= ~INIT_BIT_GLOBAL_TAGGED_METRIC_REGISTRY;
      return this;
    }

    /**
     * Initializes the value for the {@link TransactionManagers#asyncInitializationCallback() asyncInitializationCallback} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#asyncInitializationCallback() asyncInitializationCallback}.</em>
     * @param asyncInitializationCallback The value for asyncInitializationCallback
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder asyncInitializationCallback(Callback<TransactionManager> asyncInitializationCallback) {
      checkNotIsSet(asyncInitializationCallbackIsSet(), "asyncInitializationCallback");
      this.asyncInitializationCallback = Objects.requireNonNull(asyncInitializationCallback, "asyncInitializationCallback");
      optBits |= OPT_BIT_ASYNC_INITIALIZATION_CALLBACK;
      return this;
    }

    /**
     * Builds a new {@link ImmutableTransactionManagers ImmutableTransactionManagers}.
     * @return An immutable instance of TransactionManagers
     * @throws IllegalStateException if any required attributes are missing
     */
    public ImmutableTransactionManagers build() {
      checkRequiredAttributes();
      return new ImmutableTransactionManagers(this);
    }

    private boolean runtimeConfigSupplierIsSet() {
      return (optBits & OPT_BIT_RUNTIME_CONFIG_SUPPLIER) != 0;
    }

    private boolean registrarIsSet() {
      return (optBits & OPT_BIT_REGISTRAR) != 0;
    }

    private boolean lockServerOptionsIsSet() {
      return (optBits & OPT_BIT_LOCK_SERVER_OPTIONS) != 0;
    }

    private boolean allowHiddenTableAccessIsSet() {
      return (optBits & OPT_BIT_ALLOW_HIDDEN_TABLE_ACCESS) != 0;
    }

    private boolean validateLocksOnReadsIsSet() {
      return (optBits & OPT_BIT_VALIDATE_LOCKS_ON_READS) != 0;
    }

    private boolean lockImmutableTsOnReadOnlyTransactionsIsSet() {
      return (optBits & OPT_BIT_LOCK_IMMUTABLE_TS_ON_READ_ONLY_TRANSACTIONS) != 0;
    }

    private boolean allSafeForLoggingIsSet() {
      return (optBits & OPT_BIT_ALL_SAFE_FOR_LOGGING) != 0;
    }

    private boolean asyncInitializationCallbackIsSet() {
      return (optBits & OPT_BIT_ASYNC_INITIALIZATION_CALLBACK) != 0;
    }

    private boolean configIsSet() {
      return (initBits & INIT_BIT_CONFIG) == 0;
    }

    private boolean userAgentIsSet() {
      return (initBits & INIT_BIT_USER_AGENT) == 0;
    }

    private boolean globalMetricsRegistryIsSet() {
      return (initBits & INIT_BIT_GLOBAL_METRICS_REGISTRY) == 0;
    }

    private boolean globalTaggedMetricRegistryIsSet() {
      return (initBits & INIT_BIT_GLOBAL_TAGGED_METRIC_REGISTRY) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of TransactionManagers is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!configIsSet()) attributes.add("config");
      if (!userAgentIsSet()) attributes.add("userAgent");
      if (!globalMetricsRegistryIsSet()) attributes.add("globalMetricsRegistry");
      if (!globalTaggedMetricRegistryIsSet()) attributes.add("globalTaggedMetricRegistry");
      return "Cannot build TransactionManagers, some of required attributes are not set " + attributes;
    }
  }

  @Generated(from = "TransactionManagers", generator = "Immutables")
  public interface ConfigBuildStage {
    /**
     * Initializes the value for the {@link TransactionManagers#config() config} attribute.
     * @param config The value for config
     * @return {@code this} builder for use in a chained invocation
     */
    UserAgentBuildStage config(AtlasDbConfig config);
  }

  @Generated(from = "TransactionManagers", generator = "Immutables")
  public interface UserAgentBuildStage {
    /**
     * Initializes the value for the {@link TransactionManagers#userAgent() userAgent} attribute.
     * @param userAgent The value for userAgent
     * @return {@code this} builder for use in a chained invocation
     */
    GlobalMetricsRegistryBuildStage userAgent(UserAgent userAgent);
  }

  @Generated(from = "TransactionManagers", generator = "Immutables")
  public interface GlobalMetricsRegistryBuildStage {
    /**
     * Initializes the value for the {@link TransactionManagers#globalMetricsRegistry() globalMetricsRegistry} attribute.
     * @param globalMetricsRegistry The value for globalMetricsRegistry
     * @return {@code this} builder for use in a chained invocation
     */
    GlobalTaggedMetricRegistryBuildStage globalMetricsRegistry(MetricRegistry globalMetricsRegistry);
  }

  @Generated(from = "TransactionManagers", generator = "Immutables")
  public interface GlobalTaggedMetricRegistryBuildStage {
    /**
     * Initializes the value for the {@link TransactionManagers#globalTaggedMetricRegistry() globalTaggedMetricRegistry} attribute.
     * @param globalTaggedMetricRegistry The value for globalTaggedMetricRegistry
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal globalTaggedMetricRegistry(TaggedMetricRegistry globalTaggedMetricRegistry);
  }

  @Generated(from = "TransactionManagers", generator = "Immutables")
  public interface BuildFinal {

    /**
     * Initializes the value for the {@link TransactionManagers#runtimeConfigSupplier() runtimeConfigSupplier} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#runtimeConfigSupplier() runtimeConfigSupplier}.</em>
     * @param runtimeConfigSupplier The value for runtimeConfigSupplier
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal runtimeConfigSupplier(Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier);

    /**
     * Adds one element to {@link TransactionManagers#schemas() schemas} set.
     * @param element A schemas element
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal addSchemas(Schema element);

    /**
     * Adds elements to {@link TransactionManagers#schemas() schemas} set.
     * @param elements An array of schemas elements
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal addSchemas(Schema... elements);

    /**
     * Adds elements to {@link TransactionManagers#schemas() schemas} set.
     * @param elements An iterable of schemas elements
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal addAllSchemas(Iterable<? extends Schema> elements);

    /**
     * Initializes the value for the {@link TransactionManagers#registrar() registrar} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#registrar() registrar}.</em>
     * @param registrar The value for registrar
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal registrar(Consumer<Object> registrar);

    /**
     * Initializes the value for the {@link TransactionManagers#lockServerOptions() lockServerOptions} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#lockServerOptions() lockServerOptions}.</em>
     * @param lockServerOptions The value for lockServerOptions
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal lockServerOptions(LockServerOptions lockServerOptions);

    /**
     * Initializes the value for the {@link TransactionManagers#allowHiddenTableAccess() allowHiddenTableAccess} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#allowHiddenTableAccess() allowHiddenTableAccess}.</em>
     * @param allowHiddenTableAccess The value for allowHiddenTableAccess
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal allowHiddenTableAccess(boolean allowHiddenTableAccess);

    /**
     * Initializes the value for the {@link TransactionManagers#validateLocksOnReads() validateLocksOnReads} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#validateLocksOnReads() validateLocksOnReads}.</em>
     * @param validateLocksOnReads The value for validateLocksOnReads
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal validateLocksOnReads(boolean validateLocksOnReads);

    /**
     * Initializes the value for the {@link TransactionManagers#lockImmutableTsOnReadOnlyTransactions() lockImmutableTsOnReadOnlyTransactions} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#lockImmutableTsOnReadOnlyTransactions() lockImmutableTsOnReadOnlyTransactions}.</em>
     * @param lockImmutableTsOnReadOnlyTransactions The value for lockImmutableTsOnReadOnlyTransactions
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal lockImmutableTsOnReadOnlyTransactions(boolean lockImmutableTsOnReadOnlyTransactions);

    /**
     * Initializes the value for the {@link TransactionManagers#allSafeForLogging() allSafeForLogging} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#allSafeForLogging() allSafeForLogging}.</em>
     * @param allSafeForLogging The value for allSafeForLogging
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal allSafeForLogging(boolean allSafeForLogging);

    /**
     * Initializes the value for the {@link TransactionManagers#asyncInitializationCallback() asyncInitializationCallback} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TransactionManagers#asyncInitializationCallback() asyncInitializationCallback}.</em>
     * @param asyncInitializationCallback The value for asyncInitializationCallback
     * @return {@code this} builder for use in a chained invocation
     */
    BuildFinal asyncInitializationCallback(Callback<TransactionManager> asyncInitializationCallback);

    /**
     * Builds a new {@link ImmutableTransactionManagers ImmutableTransactionManagers}.
     * @return An immutable instance of TransactionManagers
     * @throws IllegalStateException if any required attributes are missing
     */
    ImmutableTransactionManagers build();
  }
}
