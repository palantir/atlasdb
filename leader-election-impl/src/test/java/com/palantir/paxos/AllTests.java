package com.palantir.paxos;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    PaxosConsensusFastTest.class,
    PaxosConsensusSlowTest.class
})
public class AllTests {
}
