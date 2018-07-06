===========================
Asynchronous Initialization
===========================

If the ``initializeAsync`` parameter of the AtlasDB configuration is set to true, AtlasDB will be able to start even when some of
the necessary resources are not ready yet. The main use of this feature is to avoid race conditions when starting multiple
services simultaneously and allow AtlasDB to start even if, e.g., the KVS is still starting up.

To simplify initialization of objects depending on a transaction manager that may be asynchronously initialized,
``TransactionManagers`` has an ``asyncInitializationCallback()`` parameter allowing users to specify a custom callback
that should be executed when ready.

If ``initializeAsync`` is set to false, the callback will be run synchronously (blocking until it is done) as the last step in creating the transaction manager.
If ``initializeAsync`` is set to true, the callback will also be run asynchronously:

1. Once all the resources for the transaction manager have become ready, **but before the transaction manager becomes initialized**, the registered callback will be run asynchronously.
2. If the callback fails and should not be retried, the transaction manager will be closed.
3. If the callback is successful, the transaction manager then becomes initialized.

Using Callbacks
---------------

For an example of using callbacks, refer to ``TargetedSweeper``. The simplest way of using this functionality is to
implement the ``CallbackInitializable`` interface. The ``initialize`` method should initialize all the
resources that require an initialized transaction manager, and the ``onInitializationFailureCleanup`` should specify any
cleanup to be done in case of failure, if necessary. The callback that should be passed to the `TransactionManagers`
builder is then given by the interface:

1. If initialization should not be retried on failure use ``singleAttemptCallback()``.
2. If initialization should be retried on failure use ``retryUnlessCleanupThrowsCallback()``.
3. If initialization should be conditionally retried on failure also use ``retryUnlessCleanupThrowsCallback()``, but the ``onInitializationFailureCleanup`` method should throw when the condition is not satisfied.
