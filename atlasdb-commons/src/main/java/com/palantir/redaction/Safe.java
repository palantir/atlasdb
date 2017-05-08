/**
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.redaction;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a parameter on a Jersey resource to be safe.
 *
 * Example:
 * <pre><code>
 *  import com.palantir.redaction.Safe;
 *  import javax.ws.rs.Get;
 *  import javax.ws.rs.Path;
 *  import javax.ws.rs.PathParam;
 *
 * {@literal @}Path("/foo")
 *  public interface Foo {
 *     {@literal @}GET
 *     {@literal @}Path("/bar/{baz}/{bop}")
 *      void doSomething(
 *          {@literal @}Safe @PathParam("baz") String baz,
 *          {@literal @}PathParam("bop") String bop);
 *  }
 * </code></pre>
 * If you sent a request to this server that looked like <code>/foo/bar/hello/world</code>, then
 * an implementation which relies on these annotations <i>must</i> guarantee that <code>hello</code>
 * will be preserved and <code>world</code> will not be preserved.
 *
 * An example output could look like one of these example:
 * <ul>
 * <li><code>/foo/bar/hello/_REDACTED_</code></li>
 * <li><code>/foo/bar/hello/{bop}</code></li>
 * <li><code>/foo/bar/hello/A7F386C91</code></li>
 * </ul>
 */
@Documented
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface Safe {

}
