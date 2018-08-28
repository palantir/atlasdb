Palantir Executors
==================
This library provides an ExecutorInheritableThreadLocal class which is intended as a drop-in replacement for the Java standard library's InheritableThreadLocal classes. One should rarely (if ever) use raw Java Threads directly for concurrency, in preference for ExecutorServices, so InheritableThreadLocals are not especially useful.

An ExecutorInheritableThreadLocal will be inherited by the thread running an Executor's task. In order to ensure ExecutorInheritableThreadLocals are propagated, you should use the static factory methods on PTExecutors rather than the Java standard library's Executors class.

This project exists separately from commons-executors to allow api projects to declare shared ExecutorInheritableThreadLocals without bringing in the dependencies from PTExecutors (jackson, guava, etc.).

Usage
-----
* Use ExecutorInheritableThreadLocal rather than ThreadLocal
* Use PTExecutors rather than Executors
