WHAT DOES buildSrc DO IN THE FIRST PLACE?
-----------------------------------------

This directory contains additional gradle plugins that are available
at build time as described in the gradle manual section titled "Build
sources in the buildSrc project".

See:

http://www.gradle.org/docs/current/userguide/organizing_build_logic.html#sec:build_sources


WHAT IS IT GOOD FOR IN THIS PARTICULAR CASE?
--------------------------------------------

At the moment, we are using the buildSrc mechanism to include a gradle
plugin that allows compiling java with ecj (the Eclipse Java Compiler)
rather than the default Oracle javac. Once this plugin shows up in an
actual release of pt-java-plugin, we can remove it here.

