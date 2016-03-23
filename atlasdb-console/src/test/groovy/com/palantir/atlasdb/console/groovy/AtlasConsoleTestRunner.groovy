package com.palantir.atlasdb.console.groovy

import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.junit.runners.Suite.SuiteClasses

@RunWith(Suite)
@SuiteClasses([TableTest, AtlasConsoleServiceWrapperTest])
class AtlasConsoleTestRunner {

}