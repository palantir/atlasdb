package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.palantir.timestamp.TimestampService;

public class TimestampServiceHealthCheckTests {
    private TimestampService timestampService = mock(TimestampService.class);

    private TimestampServiceHealthCheck timestampServiceHealthCheck;

    @Before
    public void before() {
        timestampServiceHealthCheck = new TimestampServiceHealthCheck(timestampService);
    }

    @Test
    public void healthyIfCanGetCurrentTime() throws Exception {
        when(timestampService.getFreshTimestamp()).thenReturn(12345L);
        assertThat(timestampServiceHealthCheck.check().isHealthy()).isTrue();
    }

    @Test
    public void unhealthyIfCanGetCurrentTime() throws Exception {
        when(timestampService.getFreshTimestamp()).thenThrow(new RuntimeException());
        assertThat(timestampServiceHealthCheck.check().isHealthy()).isFalse();
    }

}
