package com.palantir.atlas.impl;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.base.Throwables;

public class AtlasLoggingFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(AtlasLoggingFilter.class);

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // do nothing
    }

    @Override
    public void destroy() {
        // do nothing
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        try {
            chain.doFilter(request, response);
        } catch (Throwable t) {
            log.error("Uncaught exception", t);
            Throwables.rewrapAndThrowIfInstance(t, IOException.class);
            Throwables.rewrapAndThrowIfInstance(t, ServletException.class);
            Throwables.rewrapAndThrowUncheckedException(t);
        }
    }
}
