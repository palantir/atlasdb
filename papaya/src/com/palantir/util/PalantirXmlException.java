package com.palantir.util;


/**  We throw this exception to indicate a character &#xD; is present during cleaning a String.
 */
public class PalantirXmlException extends RuntimeException {

    public PalantirXmlException(String s) {
        super(s);
    }

    private static final long serialVersionUID = 1943556554260486306L;
}
