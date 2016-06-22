package com.palantir.atlasdb.ete.todo;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;

import com.palantir.atlasdb.ete.EteSetup;

public class CassandraTodoEteTest extends TodoEteTest {
    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition("docker-compose.cassandra.yml");
}
