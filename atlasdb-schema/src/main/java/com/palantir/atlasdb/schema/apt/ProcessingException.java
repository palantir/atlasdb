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
package com.palantir.atlasdb.schema.apt;

import javax.lang.model.element.Element;

public class ProcessingException extends Exception {

	private static final long serialVersionUID = 1L;
	Element element;

	public ProcessingException(Element element, String msg, Object... args) {
		super(String.format(msg, args));
		this.element = element;
	}

	public Element getElement() {
		return element;
	}
}