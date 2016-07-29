/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 *
 * THIS SOFTWARE CONTAINS PROPRIETARY AND CONFIDENTIAL INFORMATION OWNED BY PALANTIR TECHNOLOGIES INC.
 * UNAUTHORIZED DISCLOSURE TO ANY THIRD PARTY IS STRICTLY PROHIBITED
 *
 * For good and valuable consideration, the receipt and adequacy of which is acknowledged by Palantir and recipient
 * of this file ("Recipient"), the parties agree as follows:
 *
 * This file is being provided subject to the non-disclosure terms by and between Palantir and the Recipient.
 *
 * Palantir solely shall own and hereby retains all rights, title and interest in and to this software (including,
 * without limitation, all patent, copyright, trademark, trade secret and other intellectual property rights) and
 * all copies, modifications and derivative works thereof.  Recipient shall and hereby does irrevocably transfer and
 * assign to Palantir all right, title and interest it may have in the foregoing to Palantir and Palantir hereby
 * accepts such transfer. In using this software, Recipient acknowledges that no ownership rights are being conveyed
 * to Recipient.  This software shall only be used in conjunction with properly licensed Palantir products or
 * services.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.Test;

public class CassandraServerVersionTest {
    @Test public void
    version_19_37_0_supports_cas() {
        CassandraServerVersion version = new CassandraServerVersion("19.37.0");
        assertThat(version.supportsCheckAndSet(), is(true));
    }

    @Test public void
    version_19_36_0_does_not_support_cas() {
        CassandraServerVersion version = new CassandraServerVersion("19.36.0");
        assertThat(version.supportsCheckAndSet(), is(false));
    }

    @Test public void
    version_19_38_0_supports_cas() {
        CassandraServerVersion version = new CassandraServerVersion("19.38.0");
        assertThat(version.supportsCheckAndSet(), is(true));
    }

    @Test public void
    version_20_1_0_supports_cas() {
        CassandraServerVersion version = new CassandraServerVersion("20.1.0");
        assertThat(version.supportsCheckAndSet(), is(true));
    }

    @Test public void
    version_18_40_0_does_not_support_cas() {
        CassandraServerVersion version = new CassandraServerVersion("18.40.0");
        assertThat(version.supportsCheckAndSet(), is(false));
    }

    @Test public void
    version_20_40_1_does_support_cas() {
        CassandraServerVersion version = new CassandraServerVersion("20.40.1");
        assertThat(version.supportsCheckAndSet(), is(true));
    }

    @Test(expected=UnsupportedOperationException.class) public void
    invalid_version_strings_throw_an_error() {
        new CassandraServerVersion("20_4.1");
    }

}
