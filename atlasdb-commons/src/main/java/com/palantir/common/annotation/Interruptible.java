/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import com.palantir.annotations.PublicApi;


/**
 * This annotation should be used to annotate functions that properly handle being interrupted.  In general, these
 * methods should only call other methods which are Interruptible.  Otherwise, if a Interruptible method calls one
 * which is not interruptible in general, the specific use of that other method must be safe under interrupt.
 *
 * A method is Interruptible if:
 * 1. Interrupting the calling thread cannot possibly cause inconsistent state during the run of that method
 * 		(won't break anything).
 * 2. The method does not swallow InterruptedExceptions or the interrupted bit on its calling thread.
 * 		It either rethrows the InterruptedException or resets the interrupted bit on its calling thread (in which
 * 		case it may also throw some other exception).
 * 3. The method explicitly or implicitly checks the interrupted bit before/during expensive processing.  This will
 * 		usually be implicit (any time you use java.concurrent classes to block, or anything else that legitimately throws an
 * 		InterruptedException, will be somehow checking that bit).  However, a very expensive process that does not block
 * 		might need to check this bit explicitly.  See {@link Thread#interrupted()}.
 *
 * By definition, any @CancelableServerCall is interruptible.  We use this Interruptible annotation for cancelable
 * calls that are not necessarily server calls.
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
@PublicApi
public @interface Interruptible {
    // marker annotation

    static class Utils {
        public static void throwIfInterrupted() throws InterruptedException {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }

        public static boolean throwsInterruptedException(Method method) {
            for (Class<?> c : method.getExceptionTypes()) {
                if (InterruptedException.class.isAssignableFrom(c)) {
                    return true;
                }
            }
            return false;
        }
    }
}
