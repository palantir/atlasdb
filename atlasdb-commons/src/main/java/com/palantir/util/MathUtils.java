/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.util;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.special.Gamma;

public class MathUtils {
    // =========================
    // COMBINATORICS

    public static <T> Iterable<Object[]> mChooseN(final List<T> list, final int n) {
        // =========================
        // <code>Iterable</code> implementation
        // =========================
        return () -> mChooseNIterator( list, n );
    }

    /**
     * Returns an iterator over all possible sets of length <code>n</code> that can be made from the elements
     * in <code>list</code>.
     */
    public static <T> Iterator<Object[]> mChooseNIterator(final List<T> list, final int n) {
        final Iterator<int[]> iitr = mChooseNIndicesIterator( list.size(), n );
        return new Iterator<Object[]>() {

            // =========================
            // <code>Iterator</code> implementation

            @Override
            public Object[] next() {
                final int[] indices = iitr.next();
                final Object[] tuple = new Object[ indices.length ] ;
                    //(T[]) Array.newInstance( list.get( 0 ).getClass(), indices.length );
                //new Object[ indices.length ];
                for ( int i = 0; tuple.length > i; i++ )
                    tuple[ i ] = list.get( indices[ i ] );
                return tuple;
            }

            @Override
            public boolean hasNext() {
                return iitr.hasNext();
            }

            @Override
            public void remove() {
                iitr.remove();
            }

            // =========================
        };
    }


    public static Iterable<int[]> mChooseNIndices(final int m, final int n) {
        // =========================
        // <code>Iterable</code> implementation
        // =========================
        return () -> mChooseNIndicesIterator( m, n );
    }

    public static Iterator<int[]> mChooseNIndicesIterator(final int m, final int n) {
        if ( m < n || 0 == n )
            return Collections.<int[]>emptyList().iterator();
        else
            return new Iterator<int[]>() {
                private int[] indices;

                {
                    indices = new int[ n ];
                    for ( int i = 0; indices.length > i; indices[ i ] = i, i++ );
                }


                // =========================
                // <code>Iterator</code> implementation

                @Override
                public int[] next() {
                    final int[] _indices = new int[ indices.length ];
                    System.arraycopy( indices, 0, _indices, 0, _indices.length );

                    assert checkUnique( _indices );

                    // Update indices: {
                    // Start at the end, increment until can go no further; then go down an index, increment by one, or go down; etc.
                    for ( int i = n - 1; 0 <= i; i-- ) {
                        if ( indices[ i ] < m - n + i ) {
                            // Increment it; set all other immediately after
                            indices[ i ]++;
                            for ( int j = i + 1; n > j; j++ )
                                indices[ j ] = indices[ i ] + ( j - i );

                            return _indices;
                        }
                    }
                    // }

                    // End of run; nothing below index <code>0</code> to increment, so mark the iterator done:
                    indices[ 0 ]++;

                    return _indices;
                }

                @Override
                public boolean hasNext() {
                    return m - n >= indices[ 0 ];
                }

                @Override
                public void remove() {
                    // Do nothing
                }

                // =========================
            };
    }


    /**
     * Checks that the given indices are unique.
     */
    private static boolean checkUnique(final int[] indices) {
        final Set<Integer> indicesSet = new HashSet<Integer>();
        for ( int index : indices )
            indicesSet.add( index );
        return indices.length == indicesSet.size();
    }


    // =========================


    private static final double _2_PI = 2 * Math.PI;


    /**
     * Normalizes an angle onto <code>[0, 2 pi)</code>.
     */
    public static double norm(double t) {
//        while ( t < 0 ) t += _2_PI;
//        while ( t >= _2_PI ) t -= _2_PI;
//        return t;
        return mod( t, _2_PI );
    }

    public static double mod(double i, final double n) {
        i = 0 <= i ? i % n : ( n - ( -i % n ) ) % n;
        assert 0 <= i && i < n;
        return i;
    }

    public static int mod(int i, final int n) {
        i = 0 <= i ? i % n : ( n - ( -i % n ) ) % n;
        assert 0 <= i && i < n;
        return i;
    }

    public static long fastRound(double d) {
        if (d < 0) {
            return (long)(d-0.5);
        }
        return (long)(d+0.5);
    }

    private static final double ABSOLUTE_EPSILON = 1e-7;
    private static final double RELATIVE_EPSILON = 1e-10;

    public static boolean almostEqual(double a, double b) {
        if (Math.abs(a - b) <= ABSOLUTE_EPSILON) return true;

        double a1 = a * (1 - RELATIVE_EPSILON);
        double a2 = a * (1 + RELATIVE_EPSILON);
        if ((a1 <= b) && (b <= a2)) return true;
        if ((a2 <= b) && (b <= a1)) return true;

        double b1 = b * (1 - RELATIVE_EPSILON);
        double b2 = b * (1 + RELATIVE_EPSILON);
        if ((b1 <= a) && (a <= b2)) return true;
        if ((b2 <= a) && (a <= b1)) return true;

        return false;
    }

    private static final double DEFAULT_EPSILON = 0.001;

    /** @deprecated Use {@link #almostEqual(double, double)} instead. */
    @Deprecated public static boolean fequal(final double a, final double b) {
        return fequal( a, b, DEFAULT_EPSILON );
    }

    /** @deprecated Use {@link #almostEqual(double, double)} instead. */
    @Deprecated public static boolean fequal(final double a, final double b, final double epsilon) {
        return Math.abs( a - b ) < epsilon;
    }

    /**
     * Determines if n is a power of two.
     *
     * {@link Long#MIN_VALUE} is a power of 2 because it the argument is understood to be unsigned.
     *
     * Zero is not a power of 2.
     */
    public static boolean isPowerOfTwo(final long n) {
        if(n == 0) {
            return false;
        }
        return (n & -n) == n;
    }

    private static String getSignificant(double value, int sigFigs) {
        MathContext mc = new MathContext(sigFigs, RoundingMode.HALF_UP);
        BigDecimal bigDecimal = new BigDecimal(value, mc);
        return bigDecimal.toPlainString();
    }

    /**
     * This will take the input double and only keep the most significant digits.
     *
     * For Example if you put in .099 with one sig fig, you will get .1
     */
    public static double keepSignificantFigures(double input, int sigfigs) {
        return Double.valueOf(getSignificant(input, sigfigs));
    }

    private MathUtils() {/**/}

    public static double calculateConfidenceThatDistributionIsNotUniform(List<Long> values) {
        return 1.0 - calculateConfidenceThatDistributionIsUniform(values);
    }

    public static double calculateConfidenceThatDistributionIsUniform(List<Long> values) {
        return Gamma.regularizedGammaQ((values.size() - 1.0) / 2,
                chiSquareDistance(values) / 2);
    }

    private static double chiSquareDistance(List<Long> values) {
        double mean = values.stream().mapToLong(x -> x).reduce(0L, Long::sum) / (double) values.size();
        double distances =  values.stream().mapToDouble(Long::doubleValue)
                .reduce(0.0, (acc, nextVal) -> acc + Math.pow(nextVal - mean, 2));
        return distances / mean;
    }
}
