// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.palantir.annotations.PublicApi;


/**
 * This annotation should be used to annotate functions that when called twice will return the same value.
 *
 * These methods are naturally safe for retry logic and this annotation will most likely be used by spring
 * interface layers to do retry logic.
 *
 * The definition of idempotent is that if the method is called a second time with the same parameters,
 * the state of the world will be the same as the first call.  The same return value will also result from
 * the second call.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@PublicApi
public @interface Idempotent {
    // marker annotation
}
