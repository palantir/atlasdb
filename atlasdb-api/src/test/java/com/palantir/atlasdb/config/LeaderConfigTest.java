package com.palantir.atlasdb.config;

import static java.util.Collections.emptySet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class LeaderConfigTest {
    @Test
    public void shouldBeTheLockLeaderIfLocalServerMatchesLockLeader() {
        ImmutableLeaderConfig config = ImmutableLeaderConfig.builder()
                .localServer("me")
                .addLeaders("not me", "me")
                .quorumSize(2)
                .lockCreator("me")
                .build();

        assertThat(config.amITheLockLeader(), is(true));
    }

    @Test
    public void shouldNotBeTheLockLeaderIfLocalServerDoesNotMatchLockLeader() {
        ImmutableLeaderConfig config = ImmutableLeaderConfig.builder()
                .localServer("me")
                .addLeaders("not me", "me")
                .quorumSize(2)
                .lockCreator("not me")
                .build();

        assertThat(config.amITheLockLeader(), is(false));
    }


    @Test
    public void lockLeaderDefaultsToBeTheFirstLeader() {
        ImmutableLeaderConfig config = ImmutableLeaderConfig.builder()
                .localServer("me")
                .addLeaders("not me", "me")
                .quorumSize(2)
                .build();

        assertThat(config.lockCreator(), is("not me"));
    }

    @Test(expected = IllegalStateException.class)
    public void cannotCreateALeaderConfigWithNoLeaders() {
        ImmutableLeaderConfig.builder()
                .localServer("me")
                .leaders(emptySet())
                .quorumSize(0)
                .build();
    }
}
