package com.palantir.atlasdb.ete.todo;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;

import com.palantir.atlasdb.ete.EteSetup;

public class DbKvsTodoEteTest extends TodoEteTest {
    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition("docker-compose.dbkvs.yml");
}
