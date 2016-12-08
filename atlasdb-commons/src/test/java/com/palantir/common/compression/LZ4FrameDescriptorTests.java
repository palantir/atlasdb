/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.common.compression;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LZ4FrameDescriptorTests {

    @Test
    public void testSerialization() {
        LZ4FrameDescriptor frameDescriptor = new LZ4FrameDescriptor(true, 7);
        byte[] frameDescriptorBytes = frameDescriptor.toByteArray();

        assertEquals(LZ4Streams.FRAME_DESCRIPTOR_LENGTH, frameDescriptorBytes.length);
        assertEquals(frameDescriptor, LZ4FrameDescriptor.fromByteArray(frameDescriptorBytes));
    }
}
