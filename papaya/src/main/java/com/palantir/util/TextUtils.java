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
package com.palantir.util;

import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Toolkit;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 *
 * @author cfreeland
 *
 * @y.exclude
 */
public class TextUtils {
    public static final Logger log = LoggerFactory.getLogger(TextUtils.class);

    public static String newline = System.getProperty("line.separator");

    // about 100 MB
    public static int MAX_MEMORY_READ = 100000000;
    public static final String CHARSET_UTF_8 = "UTF-8";
    public static final String CHARSET_WINDOWS_1252 = "windows-1252";

    public static final int MAX_TOOLTIP_WIDTH = 1000;

    /** No instances allowed */
    private TextUtils() {/**/}

    /**
     * Provide pluralizations for text strings where appending "s" isn't
     * sufficient.  You only need to add something in here if the spelling
     * differs, as with the first case of entity -> entities.  Make sure both all entries are lower cased for TextUtils.pluralize to work properly
     */
    private static Map<String, String> plurals = new HashMap<String, String>();
    static {
        plurals.put("entity","entities");
        plurals.put("address", "addresses");
        //plurals.put("event", "events");
        plurals.put("person", "people");
        //plurals.put("document", "documents");
        plurals.put("business", "businesses");
        //plurals.put("ref", "refs");
        //plurals.put("reference", "references");
        //plurals.put("name", "names");
        plurals.put("entry", "entries");
        plurals.put("entity", "entities");
        // And now, the ents will go to war ... y e s ...
        //plurals.put("ent", "ents");
        //plurals.put("event", "events");
        //plurals.put("ev", "evs");
        //plurals.put("document", "documents");
        //plurals.put("doc", "docs");
        //plurals.put("object", "objects");
        //plurals.put("obj", "objs");
        plurals.put("property", "properties");
        plurals.put("category", "categories");
        //plurals.put("node", "nodes");
        plurals.put("match", "matches");
        plurals.put("this", "these");
        plurals.put("has", "have");
        // QA-14028: Don't pluralize ending prepositions in type names.
        plurals.put("as", "as");
        plurals.put("of", "of");
        plurals.put("media", "media");
        plurals.put("keywords", "keywords");
        plurals.put("approx", "approxes");
        plurals.put("multimedia", "multimedia");
        plurals.put("box", "boxes");
        plurals.put("matrix", "matrices");
    }

    /**
     *  This function pluralizes the given text and now accounts for three capitalization cases: lower case, Camel Case, and ALL CAPS.
     *  It converts the text to lower case first and looks it up in the plurals dictionary (which we assume to be all lower case now).
     *  If it does not exist, it simply appends a "s" to the word.  Then it converts the capitalization.  Also see TextUtilText.testPluralizeWithCaps().
     */
    public static String pluralize(String text) {
        if (text == null || "".equals(text)) {
            return "";
        }
        Boolean capsType = null;// null for all lower case, false for Camel Case, true for ALL UPPER CASE
        if (text.length() > 0) {
            char [] textArray = text.toCharArray();
            if (Character.isUpperCase(textArray[0])) {
                capsType = false; //Camel Case
                if (text.equals(text.toUpperCase())) {
                    capsType = true; // UPPER CASE
                }
            }
        }
        String lowerText = text.toLowerCase();
        String plural = plurals.get(lowerText) == null ? (lowerText + "s") : plurals.get(lowerText);
        if (capsType == null) {
            return plural; //lower case
        } else if (capsType == false) {
            if (plural != null && plural.length() > 0) {
                return Character.toUpperCase(plural.charAt(0)) + plural.substring(1); //Camel Case
            } else {
                assert false : "dictionary entry too short";
                return plural;
            }
        } else {
            return plural.toUpperCase(); //UPPER CASE
        }
    }

    /**
     * Pluralizes a word is count != 1. In the future, could use a
     * dictionary-based (perhaps even locale-sensitive) approach to get proper
     * pluralization for many words.
     *
     * @param s
     * @param count
     */
    public static String pluralizeWord(String s, long count) {
        if (count == 1L) {
            return s;
        }

        return pluralize(s);
    }

    //use pluralizeWord instead
    @Deprecated
    public static String conditionalDictPluralize(String text, int count) {
        return pluralizeWord(text, count);
    }

    //use pluralizeWord instead
    @Deprecated
    public static String num(final String singular, final int n) {
        return pluralizeWord(singular, n);
    }

    public static String formatCountString(String pattern, int count, String object) {
        if(count == 1) {
            return String.format(pattern, count, object);
        }
        else {
            return String.format(pattern, count, pluralize(object));
        }
    }

    public static String head(InputStream is, Charset charset, int lines) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(is, charset));
        StringBuilder rc = new StringBuilder();
        String line = null;
        int i = 0;
        String eol = System.getProperty("line.separator");
        do {
            line = br.readLine();
            if(line != null) {
                i++;
                rc.append(line).append(eol);
            }
        } while( i < lines && line != null );

        return rc.toString();
    }

    public static final Map<String, Integer> preferredEncodings;
    static {
        // these are the valid encodings that icu4j can spit out that are in the basic encoding set in rt.jar that comes with java
        Map<String, Integer> encodings = Maps.newHashMap();
        encodings.put("UTF-8", 100);
        encodings.put("UTF-16BE", 99);
        encodings.put("UTF-16LE", 98);
        encodings.put("ISO-8859-1", 97);
        encodings.put("ISO-8859-2", 96);
        encodings.put("ISO-8859-5", 95);
        encodings.put("ISO-8859-7", 94);
        encodings.put("windows-1251", 93);
        encodings.put("KOI8-R", 92);
        encodings.put("ISO-8859-9", 91);
        preferredEncodings = ImmutableMap.copyOf(encodings);
    }

    /**  This function makes an escaped name for text file loading.
     * @param filename
     */
    public static String makeEscapedName(String filename) {
        return filename.replaceAll(" ","%20").replaceAll("\\[", "%5B").replaceAll("\\]", "%5D");
//        return filename.replaceAll("\\\\","/").replaceAll(" ","%20").replaceAll("\\[", "%5B").replaceAll("\\]", "%5D");
    }

    private static Pattern blankPattern = Pattern.compile("\n\\s*\n");
    public static String removeBlanks(CharSequence buf)
    {
        Matcher m = blankPattern.matcher(buf);
        return m.replaceAll("\n");
    }

    /**
     * If regex matches the beginning of str, returns str with that matching part removed.
     * Else returns str.
     * @param regex that matches the part to be removed
     * @param str
     */
    public static String removeLeading(String regex, String str) {
        if (TextUtils.isEmpty(str)) {
            return str;
        }
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.find() && 0 == matcher.start()) {
            return str.replaceFirst(regex, "");
        }
        return str;
    }

// 	  DEPENDING on what is more convenient, added a more generic version below, feel free to delete this later!
    private static final Pattern whiteSpacePattern = Pattern.compile("\\s");
    private static final Pattern newlinePattern = Pattern.compile("\n");
    private static final Pattern spacePattern = Pattern.compile(" ");
    private static final Pattern commaPattern = Pattern.compile(",");
    private static final Pattern bracesPattern = Pattern.compile("[\\{\\}]");

    public static String[] splitByWhiteSpaces(String string)
    {
        return whiteSpacePattern.split(string);
    }

    public static String[] splitBySpace(String string)
    {
        return spacePattern.split(string);
    }

    public static String[] splitByNewline(String string)
    {
        return newlinePattern.split(string);
    }

    public static String[] splitByComma(String string)
    {
        return commaPattern.split(string);
    }

    public static String[] splitByBraces(String string)
    {
        return bracesPattern.split(string);
    }

    /**
     * Cached pattern split!
     * Does a String class split while caching the patterns so they do not need to be
     * recompiled each time.
     * @param string - the string to be split
     * @param regex - the regular expression to use for the split
     */
    public static String[] split(String string, String regex)
    {
        return getPattern(regex).split(string);
    }

    /**
     * Cached pattern replace all!
     * @param string
     * @param replaceValue
     * @param regex
     */
    public static String replaceAll(String string, String regex, String replaceValue)
    {
        return getPattern(regex).matcher(string).replaceAll(replaceValue);
    }

    // assuming that only a small finite number of expressions used here, otherwise need a soft cache
    private static final Map<String, Pattern> patternMap = new HashMap<String, Pattern>();
    private static Pattern getPattern(String regex)
    {
        if (patternMap.containsKey(regex))
        {
            return patternMap.get(regex);
        }
        else
        {
            Pattern p = Pattern.compile(regex);
            patternMap.put(regex, p);
            return p;
        }

    }

    private static final NumberFormat nfCurrency = NumberFormat.getNumberInstance();
    private static final NumberFormat nfInteger = NumberFormat.getIntegerInstance();
    private static final NumberFormat nfDouble = NumberFormat.getNumberInstance();
    static {
        // show two decimals for all currencies
        nfCurrency.setMinimumFractionDigits(2);
        // allow as many decimical digits as possible
        nfDouble.setMaximumFractionDigits(Integer.MAX_VALUE);
    }
    /**
     * Adds commas to the thousands, millionth place etc and then returns
     * the string representation of the given integer.
     * @param integer
     */
    public static String formatInteger(int integer)
    {
        return nfInteger.format(integer);
    }

    public static String formatCurrencyAmount(double d)
    {
        return nfCurrency.format(d);
    }

    public static String formatDouble(double d)
    {
        return nfDouble.format(d);
    }

    private static Vector<ResourceBundle> mResources = new Vector<ResourceBundle>();

    public static void addResources(String resourceName, Locale locale) {
        mResources.add(ResourceBundle.getBundle(resourceName, locale));

    }

    public static String getResource(String resourceName) {
        ResourceBundle rb;
        String value = null;
        for(int i = 0; value == null && i < mResources.size(); i++) {
            rb = mResources.get(i);
            try {
                value = rb.getString(resourceName);
            }
            catch(MissingResourceException mrex) {
                value = null;
            }
        }
        return value;
    }

    public static final String PROPERTY_LIST_DELIMITER = "\\|";

    public static String [] getResourceArray(String resourceName) {
        String value = getResource(resourceName);
        if(value != null) {
            // Strip off the leading and trailing "|" which I require because
            // you need to be able to specify whitespace as a value in the first
            // or last position.
            value = value.substring(1, value.length()-1);
            return value.split(PROPERTY_LIST_DELIMITER);
        }

        return null;
    }


    /** MMMMM dd */
    public static final int MONTH_DATE_FORMAT = 0;
    /** MMMMMMMMM */
    public static final int MO_DAY_DATE_FORMAT = 1;
    /** MM/dd/yy */
    public static final int SHORT_DATE_FORMAT = 2;
    /** MM/dd/yyyy */
    public static final int SHORT_DATE_FORMAT_FOUR_YEAR = 3;
    /** MMMMMM dd, yyyy */
    public static final int MEDIUM_DATE_FORMAT = 4;
    /** MMMMM dd, yyyy hh:mm aaa */
    public static final int LONG_DATETIME_FORMAT = 5;
    /** hh:mm */
    public static final int SHORT_TIME_FORMAT = 6;
    /** hh:mm:ss */
    public static final int LONG_TIME_FORMAT = 7;
    /** yyyy */
    public static final int YEAR_DATE_FORMAT = 8;
    /** MM/dd/yyyy HH:mm z*/
    public static final int SHORT_DATETIME_FORMAT = 9;
    /** MM/dd/yyyy HH:mm:ss z */
    public static final int SHORT_DATETIME_FORMAT_WITH_SECONDS = 10;
    /** MMMMMM dd, yyyy HH:mm*/
    public static final int MEDIUM_DATETIME_FORMAT = 11;
    /** MM/dd/yyyy HH:mm */
    public static final int SHORTER_DATETIME_FORMAT = 12;
    /** YYYY/MM **/
    public static final int GROUP_BY_MONTH_FORMAT = 13;
    public static final int SHORTEST_DATETIME_FORMAT = 14;

    /*
     * SimpleDateFormat is not threadsafe, so we encapsulate it in thread local storage.
     */

    private final static ThreadLocal<SimpleDateFormat> shortTimeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("HH:mm"); } };
    private final static ThreadLocal<SimpleDateFormat> longTimeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("HH:mm:ss"); } };

    private final static ThreadLocal<SimpleDateFormat> monthDayDateFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MMMMM"); } };
    private final static ThreadLocal<SimpleDateFormat> monthDateFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MMMMM dd"); } };
    private final static ThreadLocal<SimpleDateFormat> yearDateFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("yyyy"); } };

    private final static ThreadLocal<SimpleDateFormat> shortDateFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MM/dd/yy"); } };
    private final static ThreadLocal<SimpleDateFormat> shortestDateTimeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("HH:mm MM/dd/yy"); } };
    private final static ThreadLocal<SimpleDateFormat> shortDateTimeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MM/dd/yyyy HH:mm z"); } };
    private final static ThreadLocal<SimpleDateFormat> shortDateTimeFormatWithSeconds = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z"); } };
    private final static ThreadLocal<SimpleDateFormat> shorterDateTimeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MM/dd/yyyy HH:mm"); } };
    private final static ThreadLocal<SimpleDateFormat> mediumDateFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MMMMM dd, yyyy"); } };
    private final static ThreadLocal<SimpleDateFormat> longDateTimeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MMMMM dd, yyyy hh:mm aaa"); } };
    private final static ThreadLocal<SimpleDateFormat> mediumDateTimeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MMMMMM dd, yyyy HH:mm"); } };
    private final static ThreadLocal<SimpleDateFormat> shortDateFormatFourYear = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("MM/dd/yyyy"); } };

    private final static ThreadLocal<SimpleDateFormat> groupByMonthFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() { return new SimpleDateFormat("yyyy/MM"); } };

    /**
     * Converts the given date to a String with the requested date format.
     *
     * @param d - Date to convert
     * @param format - the format as an int, see the TextUtils.<type>_FORMAT constants
     * @return a string representing the date in the desired format
     * @deprecated replaced by {@link com.palantir.util.i18n.DateFormatterFactory}
     */
    @Deprecated
    public static String convertDate(Date d, int format)
    {
        return getThreadLocalDateFormatter(format).format(d);
    }

    /**
     * Converts the given long to a String with the requested date format.
     *
     * @param time - time in MS since Jan 1, 1970 to convert to a String
     * @param format - the format as an int, see the TextUtils.<type>_FORMAT constants
     *
     * @return a string representing the date in the desired format
     * @deprecated replaced by {@link com.palantir.util.i18n.DateFormatterFactory}
     */
    @Deprecated
    public static String convertDateFromMS(long time, int format)
    {
        Date d = new java.util.Date(time);
        return convertDate(d, format);
    }

    /**
     * Converts the given long to a String with the requested date format.
     *
     * @param time - time in seconds since Jan 1, 1970 to convert to a String
     * @param format - the format as an int, see the TextUtils.<type>_FORMAT constants
     *
     * @return a string representing the date in the desired format
     * @deprecated replaced by {@link com.palantir.util.i18n.DateFormatterFactory}
     */
    @Deprecated
    public static String convertDateFromSeconds(long time, int format)
    {
        Date d = new java.util.Date(time * 1000L);
        return convertDate(d, format);
    }
    /**
     * Returns a DateFormat that is local to the calling thread.
     *
     * @param format the date format required, will default to the SHORT_DATE_FORMAT
     *    instance if the specified format is invalid.
     *
     * @return a DateFormat that is local to this thread
     * @deprecated replaced by {@link com.palantir.util.i18n.DateFormatterFactory}
     */
    @Deprecated
    private static DateFormat getThreadLocalDateFormatter(int format)
    {
        switch(format) {
            case YEAR_DATE_FORMAT:
                return yearDateFormat.get();
            case MONTH_DATE_FORMAT:
                return monthDateFormat.get();
            case MO_DAY_DATE_FORMAT:
                return monthDayDateFormat.get();
            case SHORT_TIME_FORMAT:
                return shortTimeFormat.get();
            case LONG_TIME_FORMAT:
                return longTimeFormat.get();
            case MEDIUM_DATE_FORMAT:
                return mediumDateFormat.get();
            case LONG_DATETIME_FORMAT:
                return longDateTimeFormat.get();
            case SHORT_DATETIME_FORMAT:
                return shortDateTimeFormat.get();
            case SHORT_DATETIME_FORMAT_WITH_SECONDS:
                return shortDateTimeFormatWithSeconds.get();
            case MEDIUM_DATETIME_FORMAT:
                return mediumDateTimeFormat.get();
            case SHORTER_DATETIME_FORMAT:
                return shorterDateTimeFormat.get();
            case SHORTEST_DATETIME_FORMAT:
                return shortestDateTimeFormat.get();
            case GROUP_BY_MONTH_FORMAT:
                return groupByMonthFormat.get();
            case SHORT_DATE_FORMAT_FOUR_YEAR:
                return shortDateFormatFourYear.get();
            case SHORT_DATE_FORMAT:
            default:
                return shortDateFormat.get();
        }
    }

    /**
     * Will attempt to parse the given string using the requested format. If
     * the String does not parse with the desired pattern, null will be returned.
     *
     * Note - partial matches are not supported.  So if asked to parse a String with the format:
     * "MM/dd/yy HH:mm:ss z"
     *
     * with the pattern:
     * "MM/dd/yy HH:mm"
     *
     * Even though it matches, we will not consider this a full parse and wil return null.
     *
     * @param str the String to Parse
     * @param format the format to Parse the String as
     *
     * @return a new Date object, or null if the parse failed
     * @deprecated
     */
    @Deprecated
    public static Date parseDateTimeExact(String str, int format)
    {
        return parseDateTime(str, format, true);
    }

    /**
     * Will attempt to parse the given string using the requested format. If
     * the String does not parse with the desired pattern, null will be returned.
     *
     * @param str the String to Parse
     * @param format the format to Parse the String as
     * @param requireExactMatch set to true to require an exact format match, false to allow
     *    partial matches.
     *
     * @return a new Date object, or null if the parse failed
     * @deprecated
     */
    @Deprecated
    public static Date parseDateTime(String str, int format, boolean requireExactMatch)
    {
        DateFormat parser = getThreadLocalDateFormatter(format);

        ParsePosition pos = new ParsePosition(0);
        Date date = parser.parse(str, pos);

        // is there a match?
        if (date != null) {
            // do we require an exact match?
            if(requireExactMatch)
            {
                // ensure exact match
                if(pos.getIndex() == str.length())
                {
                    return date;
                }
            } else {
                return date;
            }
        }

        return null;
    }

    /**
     * Formats a number of seconds as H:MM:SS where the number of hours can be arbitrarily
     * large but the number of minutes and seconds are always two digits.
     * @param seconds
     * @return
     */
    public static String formatTime(long seconds) {
        return String.format("%d:%02d:%02d",
                seconds / (60 * 60), // hours
                (seconds / 60) % 60, // minutes
                seconds % 60); // seconds
    }

    /*
     * TODO (dchiu) decide what is better to use to add commas to currency
     * amounts, this method or the formatCurrencyAmount() above in this class
     */
    public static String formatAmount(String samt)
    {
        String sgn="";

        double damt = (new Double(samt)).doubleValue()/100.0;

        if(damt<0)
        {
            sgn="-";
            damt=-damt;
        }

        int ds = (int)damt;
        int cs = (int)(100.0*(damt - ds));

        return sgn + ds + "." + (cs/10) + (cs%10);
    }

    public static String formatAmount(String currency, String samt)
    {
        if(currency.equals("USD"))
        {

            return "USD " + formatAmount(samt);

        } else {

            return currency + " " + samt;
        }
    }

    public static String formatAmount(String currency, String samt, int nd)
    {
        if(currency.equals("USD"))
        {
            String sgn="";

            double damt = (new Double(samt)).doubleValue()/100.0;

            if(damt<0)
            {
                sgn="-";
                damt=-damt;
            }

            int ds = (int)damt;
            int cs = (int)(100.0*(damt - ds));

            String fcamt = sgn + ds;
            while(fcamt.length()<nd) {
                fcamt = " " + fcamt;
            }

            return "USD " + fcamt + "." + (cs/10) + (cs%10);

        } else {

            String fcamt = samt;
            while(fcamt.length()<nd) {
                fcamt = " " + fcamt;
            }

            return currency + " " + samt;
        }
    }

    private static String formatPhoneNumber(String pn, String fmt)
    {
        int ci = pn.indexOf(':');
        if(ci < 0) {
            return pn;
        }

        String cc = pn.substring(0, ci);
        String rest = pn.substring(ci,pn.length()).replaceAll(":","");

        String fmtPn="";
        int j=0;
        for(int i=0; i < fmt.length(); i++)
        {
            char ch = fmt.charAt(i);

            if(ch=='c')
            {
                fmtPn += cc;

            } else if(ch=='d') {

                if(j < rest.length())
                {
                    fmtPn += rest.charAt(j);
                    j++;
                }

            } else {

                fmtPn += ch;

            }
        }
        return fmtPn;
    }

    public static String convertPhoneNumber(String pn)
    {
        int colon = pn.indexOf(':');
        if (colon != -1)
        {
            String cc = pn.substring(0, colon);

            if(cc.equals("1")) // US
            {
                //pn = pn.replaceFirst("1:","").replaceAll(":","-");
                //pn = pn.substring(0,pn.length()-4) + "-" + pn.substring(pn.length()-4);
                pn = formatPhoneNumber(pn,"1-ddd-ddd-dddd");

            }
            else if(cc.equals("90"))
            { // turkey

                pn = formatPhoneNumber(pn,"+c dddddddddddd");

            }
            else if(cc.equals("44"))
            { // uk

                pn = formatPhoneNumber(pn,"+c dddddddddddd");

            }
            else if(cc.equals("30"))
            { // switzerland

                pn = formatPhoneNumber(pn,"+c ddddddddd");

            }
            else
            { // intl

                //pn = "+" + pn.replaceFirst(":"," ").replaceAll(":","-");
                pn = formatPhoneNumber(pn,"+c dddddddddddd");
            }
        }
        //return pn.replaceAll("--","-");
        return pn;

    }


    /**
     * Changes a string to mixed case.  (First upper, then lower.)
     */
    public static String toMixedCase(String str) {
        StringBuilder strbuf = new StringBuilder(str.toLowerCase());
        strbuf.setCharAt(0, Character.toUpperCase(strbuf.charAt(0)));
        return strbuf.toString();
    }

    /**
     * Returns a String which is the human-readable form of the number.
     * Not very well implemented right now.
     */

    private static int[] thresholds = new int[] {
        (int)Math.pow(2, 30),
        (int)Math.pow(2, 20),
        (int)Math.pow(2, 10),
        1
    };

    private static String[] thresholdSuffixes = new String[] {
        "GB",
        "MB",
        "KB",
        "B"
    };

    public static String getHumanReadableSize(long num) {
        for (int i = 0; i != thresholds.length; i++) {
            int threshold = thresholds[i];
            String suffix = thresholdSuffixes[i];
            if (num >= threshold) {
                return Long.toString(num/threshold+((num % threshold)*2 >= threshold ? 1 : 0) ) + suffix;
            }
        }

        // since last threshold is 0, this means that we have a negative number.
        // so do something reasonable.
        //assert false : "Should not pass negative number to getHumanReadableSize";
        return Long.toString(num);
    }

    /** Makes a name that doesn't match any in the set given
     */
    public static String makeUniqueName(String name, Set<String> names)
    {
        int suffixNumber = 1;
        String suffix = "";
        while (names.contains(name + suffix))
        {
            suffixNumber++;
            suffix = "_" + suffixNumber;
        }
        return name + suffix;
    }

    public static String listToString(List<?> objs)
    {
        try
        {
            CharArrayWriter writer = new CharArrayWriter();
            for(int i=0; i < objs.size(); i++)
            {
                Object obj = objs.get(i);
                if(i>0) {
                    writer.write(',');
                }
                if(obj instanceof String)
                {
                    writer.write('\'');
                    writer.write(obj.toString());
                    writer.write('\'');

                } else {

                    writer.write(obj.toString());
                }
            }
            return writer.toString();

        } catch(IOException e) {

            return null;
        }
    }

    private static final Pattern wordDelimiterPattern = Pattern.compile("[ ,\\.-]+");

    /**
     * Takes a set of comma/space/period/dash-delimited words and capitalizes the first
     * letter of each word.  All preceding letters are lowercase.  This does not
     * attempt to capitalize a sentence properly.  Instead it capitalizes the first letter of
     * each word.  For example, the input "the brown fox jumps quickly. alhambra, ca"
     * will display as "The Brown Fox Jumps Quickly. Alhambra, CA".
     * @param s - the input phrase to capitalize properly
     * @return the properly cased phrase
     */
    public static String properCaseWords(String s) {
        if (s == null || s.length() == 0) {
            return "";
        }
        StringBuilder sNew = new StringBuilder(s.length());
        Matcher m = wordDelimiterPattern.matcher(s);
        while (m.find()) {
            int start = m.start();
            int end = m.end();
            sNew.append(properCaseWord(s.substring(sNew.length(), start)));
            sNew.append(s.substring(start, end));
        }
        if (sNew.length() < s.length()) {
            sNew.append(properCaseWord(s.substring(sNew.length())));
        }
        return sNew.toString();
    }

    /**
     * Capitalizes the first letter of a word, lower-cases all other letters.
     */
    public static String properCaseWord(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder sNew = new StringBuilder(s.length());

        if (s.length() > 0) {
            // took out code to capitalize directions - QA-35789 - issue with horizon
            if (s.equals("I") || s.equals("II") || s.equals("III") || s.equals("IV")
                    || s.equals("V")) {
                return s;
            }
            if (s.equals("ii")) {
                return "II";
            }
            if (s.equals("iii")) {
                return "III";
            }
            if (s.equals("iv")) {
                return "IV";
            }
            // check if it looks like a flight, ie text is all caps 2-3 letters
            // and has a 2-3 digit number
            if (s.length() > 4 && s.toUpperCase().equals(s)) {
                // looks like AA102
                if (Character.isLetter(s.charAt(0)) && Character.isLetter(s.charAt(1))
                        && (Character.isDigit(s.charAt(2)) || Character.isLetter(s.charAt(2)))
                        && Character.isDigit(s.charAt(3)) && Character.isDigit(s.charAt(4))) {
                    return s;
                }
            }
            sNew.append(s.substring(0, 1).toUpperCase());
            if (s.length() > 1) {
                sNew.append(s.substring(1).toLowerCase());
            }
        }
        return sNew.toString();
    }


    /**
     * Returns either &quot;a&quot; or &quot;an&quot; to match the object.
     */
    // TODO (bcolwell) proper acronym support
    public static String aan(final String object) {
        if ( null == object || object.length() <= 0 ) {
            return "a";
        }

        switch ( object.charAt( 0 ) ) {
            case 'a':
            case 'e':
            case 'i':
            case 'o':
            case 'u':
            case 'A':
            case 'E':
            case 'I':
            case 'O':
            case 'U':
                return "an";
            case 'S':
                if ( object.toUpperCase().equals( object ) ) {
                    // This case is an acronyn that starts with S:
                    return "an";
                }
                break;
        }
        return "a";
    }


    /**
     *  Makes a list of the n strings, separated by the given delimiter.
     */

    public static CharSequence makeStringList(List<?> strList, String delimiter) {
        int count = strList.size();
        if (count == 0) {
            return "";
        }

        StringBuilder buf = new StringBuilder(10*count + delimiter.length()*(count-1));

        for (int i = 0; i < count-1; i++) {
            buf.append(strList.get(i));
            buf.append(delimiter);
        }
        buf.append(strList.get(count-1));

        return buf;
    }


    /**
     *  Makes a list of n copies of the given string, separated by the given delimiter.
     */

    public static String makeStringList(String str, String delimiter, int count) {
        if (count == 0) {
            return "";
        }
        StringBuilder buf = new StringBuilder(str.length()*count + delimiter.length()*(count-1));

        for (int i = 0; i < count-1; i++) {
            buf.append(str);
            buf.append(delimiter);
        }
        buf.append(str);

        return buf.toString();
    }

    public static final String SPECIFIER_0 = "\\Q{0}\\E";
    public static final String SPECIFIER_1 = "\\Q{1}\\E";
    public static final String SPECIFIER_2 = "\\Q{2}\\E";
    public static final String SPECIFIER_3 = "\\Q{3}\\E";
    public static final String SPECIFIER_4 = "\\Q{4}\\E";

    public static String format(String formatStr, String arg0) {
        return formatStr.replaceAll(SPECIFIER_0, arg0);
    }

    public static String format(String formatStr, String arg0, String arg1) {
        String temp = formatStr;
        temp = temp.replaceAll(SPECIFIER_0, arg0);
        temp = temp.replaceAll(SPECIFIER_1, arg1);
        return temp;
    }

    public static String format(String formatStr, String arg0, String arg1, String arg2) {
        String temp = formatStr;
        temp = temp.replaceAll(SPECIFIER_0, arg0);
        temp = temp.replaceAll(SPECIFIER_1, arg1);
        temp = temp.replaceAll(SPECIFIER_2, arg2);
        return temp;

    }

    public static String format(String formatStr, String arg0, String arg1, String arg2, String arg3) {
        String temp = formatStr;
        temp = temp.replaceAll(SPECIFIER_0, arg0);
        temp = temp.replaceAll(SPECIFIER_1, arg1);
        temp = temp.replaceAll(SPECIFIER_2, arg2);
        temp = temp.replaceAll(SPECIFIER_3, arg3);
        return temp;

    }

    public static String format(String formatStr, String arg0, String arg1, String arg2, String arg3, String arg4) {
        String temp = formatStr;
        temp = temp.replaceAll(SPECIFIER_0, arg0);
        temp = temp.replaceAll(SPECIFIER_1, arg1);
        temp = temp.replaceAll(SPECIFIER_2, arg2);
        temp = temp.replaceAll(SPECIFIER_3, arg3);
        temp = temp.replaceAll(SPECIFIER_4, arg4);
        return temp;

    }

    private static Pattern whitespacePattern = Pattern.compile("\\s");
    public static String breakLinesWrap(String contents, int maxLineLen, boolean useHTMLBreak) {
        if (contents == null) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        int currentLineSpace = maxLineLen;
        String[] words = whitespacePattern.split(contents);
        for (String word : words) {
            if (word.length() < currentLineSpace) {
                result.append(word).append(" ");
                currentLineSpace -= (word.length() + 1);
            }
            else {
                if (useHTMLBreak) {
                    result.append("<br>").append(word).append(" ");
                } else {
                    result.append('\n');
                }
                currentLineSpace = maxLineLen - (word.length() + 1);
            }
        }
        return result.toString();
    }

    /**
     * Used to get the XML representation of a Properties object as a String.
     *
     * @param p a valid Properties object.  If the object is null, an empty string is returned.
     * @return the XML representation of the provided Properties object.
     */
    public static String storePropertiesToXMLString(Properties p)
    {
        String xmlString;
        // null check
        if(p == null)
        {
            return "";
        }

        // write the properties file to a byte array output stream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try{
            p.storeToXML(baos, "", CHARSET_UTF_8);
            xmlString = baos.toString(CHARSET_UTF_8);
        } catch (IOException ioe)
        {
            throw new IllegalArgumentException("Error trying to convert provided properties to XML String.", ioe);
        } finally {
            try{
                baos.close();
            } catch (IOException ignored) { /* ignore */ }
        }

        return xmlString;
    }

    /**
     * Used to get the rehydrate a Properties object from it's xml form.
     *
     * @param sPropXML the properties XML.  if the string is null or empty, an empty Properties
     *        oject is instantitated and returned.
     * @return a valid Properties with the given XML loaded into it
     */
    public static Properties loadPropertiesFromXMLString(String sPropXML)
    {
        // null check
        if(sPropXML == null || sPropXML.length() == 0)
        {
            return new Properties();
        }

        // write the properties file to a byte array output stream
        Properties p = new Properties();
        ByteArrayInputStream bais = null;
        try{
            bais = new ByteArrayInputStream(sPropXML.getBytes(CHARSET_UTF_8));
            p.loadFromXML(bais);
        } catch (IOException ioe)
        {
            throw new IllegalArgumentException("Error trying to load provided XML String into a Properties object.", ioe);
        } finally {
            try{
                if(bais != null) {
                    bais.close();
                }
            } catch (IOException ignored) { /* ignore */ }
        }

        return p;
    }


    public static final String TRUNCATE_DOTDOTDOT = "...";
    /**
     * Truncates the given label string adding ... to the end of it.
     * @param str
     * @param len
     */
    public static final String truncateLabelString(String str, int len){
        return truncateLabelString(str, len, "UTF-8");
    }

    public static final String truncateLabelString(String string, int length, String encoding) {
        if(isEmpty(string)) {
            return string;
        } else {
            int lengthBeforeTruncate = computeByteLength(string, encoding);
            if(lengthBeforeTruncate > length) {
                int dotDotDotLength = computeByteLength(TRUNCATE_DOTDOTDOT, encoding);
                return truncateStringToByteLength(string, length-dotDotDotLength, encoding) + TRUNCATE_DOTDOTDOT;
            } else {
                return string;
            }
        }
    }


    public static final String TRUNCATE_LONG_START = "... [truncated at ";
    public static final String TRUNCATE_LONG_END = " characters]";

    /**
     * This function will convert a money to double...
     *
     * @param money
     */
    private static final Pattern moneyPattern = Pattern.compile("[^0-9.]*");
    public static double getMoneyAmount(String money)
    {
        if(money == null)
        {
            return 0;
        }

        Matcher matcher = moneyPattern.matcher(money);
        String amount = matcher.replaceAll("");
        try{
            return(Double.parseDouble(amount));
        } catch(NumberFormatException e)
        {
            // ignore, return 0...
        }
        return 0L;
    }


    /**
     * Given a set of Strings, it returns the maximal prefix among them all.
     * Ex: an input of {"abcd", "abfg","a123"} would return "a".
     *
     * @param strings
     */
    public static String findMaximalPrefix(Collection<String> strings){
        if(strings == null || strings.size() == 0) {
            return "";
        }

        Iterator<String> iter = strings.iterator();

        String s1 = iter.next();
        String s0 = null;

        int current_max = s1.length();
        while(iter.hasNext()){
            s0 = s1;
            s1 = iter.next();
            int len = 0;
            int limit = Math.min(current_max, s1.length());
            while((len < limit) && s0.charAt(len) == s1.charAt(len)) {
                len++;
            }
            if(len < current_max) {
                current_max = len;
            }
            if(current_max == 0) {
                break;
            }
        }
        return s1.substring(0, current_max);
    }


    /**
     * Generates a hex string from a byte array.  byte array must
     * have even number of slots.  Array is considered to be high-byte
     * first.
     *
     * @param bytes
     */
    public static String byteArrayToHexString(byte bytes[]){


        StringBuilder out = new StringBuilder();
        for(int i = 0 ; i < bytes.length ; i++){
            int highBits = (bytes[i] &0xf0) >> 4;
            int lowBits  = bytes[i] & 0xf;


            if(highBits > 9){
                switch (highBits) {
                case 10:
                    out.append("a");
                    break;
                case 11:
                    out.append("b");
                    break;
                case 12:
                    out.append("c");
                    break;
                case 13:
                    out.append("d");
                    break;
                case 14:
                    out.append("e");
                    break;
                case 15:
                    out.append("f");
                    break;
                default:
                    break;
                }
            }
            else{
                out.append(highBits);
            }

            if(lowBits > 9){
                switch (lowBits) {
                case 10:
                    out.append("a");
                    break;
                case 11:
                    out.append("b");
                    break;
                case 12:
                    out.append("c");
                    break;
                case 13:
                    out.append("d");
                    break;
                case 14:
                    out.append("e");
                    break;
                case 15:
                    out.append("f");
                    break;
                default:
                    break;
                }
            }
            else{
                out.append(lowBits);
            }

        }

        return out.toString();
    }

    /**
     * If string is longer than length, truncates it and adds the suffix on the end such that the return value is
     * of the given length.  In most cases, suffixIfTruncated will either be "" or "...".
     *
     * This method is not appropriate for truncating strings that are to be put in the database,
     * because it uses character length and not byte length.  See trunacteLabelString.
     */
    public static String truncateStringToCharLength(String string, int length, String suffixIfTruncated) {
        if (string.length() <= length) {
            return string;
        }
        Preconditions.checkArgument(suffixIfTruncated.length() <= length);
        return string.substring(0, length - suffixIfTruncated.length()) + suffixIfTruncated;
    }

    // =========================
    // BYTE LENGTH

    public static String truncateStringToByteLength(final String string, int byteLength, final String encoding)
    {
        return truncateStringToByteLength(string, byteLength, encoding, false);
    }

    public static String truncateStringToByteLength(final String string, int byteLength, final String encoding, boolean logErrorOnTruncation)
    {
        final int shaveBytes = computeByteLength( string, encoding ) - byteLength;
        final String tstring = shaveBytes <= 0 ? string : shaveStringByBytes( string, shaveBytes, encoding );
        if (shaveBytes>0 && logErrorOnTruncation) {
            log.error("String too long: truncating");
        }

        assert computeByteLength( tstring, encoding ) <= byteLength;

        if (shaveBytes > 0) {
            assert computeByteLength( tstring, encoding ) >= byteLength - 3 : "Shaved off too much";
        }

        return tstring;
    }



    public static int computeByteLength(final String string, final String encoding) {
        if(string == null) {
            return 0;
        }

        try {
            return string.getBytes( encoding ).length;
        }
        catch ( UnsupportedCharsetException e ) {
            log.error("Unsupported charset: " + encoding, e);
        }
        catch ( UnsupportedEncodingException e ) {
            log.error("Unsupported encoding: " + encoding, e);
        }
        // Unknown character set or some other problem -- just use the char length:
        return string.length();
    }


    public static int computeByteLength(final char c, final String encoding) {
        return computeByteLength( String.valueOf( c ), encoding );
    }

    public static int computeByteLength(final int i, final String encoding) {
        return computeByteLength( new String( new int[] { i }, 0, 1 ), encoding );
    }


    public static String shaveStringByBytes(final String string, int shaveBytes, final String encoding) {
        if(string == null) {
            return string;
        }

        int i = string.length();
        while ( shaveBytes > 0 && i > 0 ) {
            // use Character method instead of String method due to JDK5 bug: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6242664
            i = Character.offsetByCodePoints( string, i, -1 );
            shaveBytes -= computeByteLength( string.codePointAt( i ), encoding );
        }
        return string.substring( 0, i );
    }

    // =========================

    /**
     * Returns a string in which unnecessary whitespace is removed. Leading and trailing whitespace,
     * is removed, and multiple whitespace characters within the text are replaced with a single space.
     *
     * removeUnnecessaryWhiteSpace(" abc ") = "abc"
     * removeUnnecessaryWhiteSpace("a b c") = "a b c"
     * removeUnnecessaryWhiteSpace("a\n\rb\n\rc") = "a b c"
     * removeUnnecessaryWhiteSpace("a\tb\tc") = "a b c"
     * removeUnnecessaryWhiteSpace("abc\n") = "abc"
     * removeUnnecessaryWhiteSpace("a        b\n\n\n\n\nc") = "a b c"
     *
     * @param text
     */
    public static String removeUnnecessaryWhiteSpace(String text) {
        return replaceAllWhitespace(text, " ");
    }

    private static String replaceAllWhitespace(String text, String newSeparator) {
        StringBuilder sb = new StringBuilder();
        StringTokenizer st = new StringTokenizer(text);

        if (st.hasMoreTokens()) {
            sb.append(st.nextToken());
        }
        while (st.hasMoreTokens()) {
            sb.append(newSeparator);
            sb.append(st.nextToken());
        }

        return sb.toString();
    }

    public static String removeAllWhitespace(String text) {
        return replaceAllWhitespace(text, "");
    }


    public static String escapeHtml(String string) {
        return escapeHtml( string, true );
    }

    /**
     * Escapes HTML in the given text.  Uses <code>&amp;nbsp;</code> to preserve repeated spaces in
     * the input.  Either strips all newlines or replaces them with &quot;&lt;br/&gt;&quot;
     *
     * @param consumeNewLines	<code>true</code> if newlines should be consumed;
     * 							<code>false</code> if they should be replaced with &quot;&lt;br/&gt;&quot;
     *
     * <p>Many thanks to: http://www.rgagnon.com/javadetails/java-0306.html</p>
     */
    /* This does not correctly escape unicode characters that take more than 2 bytes to store.
     * Instead, it escapes each 2 byte character separately.  For instance, the 4-byte character
     * ð��€ should be escaped as "&#119808;", but it is escaped as "&#55349;&#56320;" (obtained by
     * directly escaping the two UTF-16 characters that make up the 4-byte character).
     */
    public static String escapeHtml(String string, final boolean consumeNewLines) {
        if ( null == string ) {
            return null;
        }

        StringBuilder sb = new StringBuilder(string.length());
        // true if last char was blank
        boolean lastWasBlankChar = false;
        int len = string.length();
        char c;

        for (int i = 0; i < len; i++)
            {
            c = string.charAt(i);
            if (c == ' ') {
                // blank gets extra work,
                // this solves the problem you get if you replace all
                // blanks with &nbsp;, if you do that you loss
                // word breaking
                if (lastWasBlankChar) {
                    lastWasBlankChar = false;
                    sb.append("&nbsp;");
                    }
                else {
                    lastWasBlankChar = true;
                    sb.append(' ');
                    }
                }
            else {
                lastWasBlankChar = false;
                //
                // HTML Special Chars
                if (c == '"') {
                    sb.append("&quot;");
                } else if (c == '&') {
                    sb.append("&amp;");
                } else if (c == '<') {
                    sb.append("&lt;");
                } else if (c == '>') {
                    sb.append("&gt;");
                } else if (c == '\n') {
                    // Handle Newline
                    if (! consumeNewLines ) {
                        sb.append("<br/>");
                    }
                }
                else {
                    int ci = 0xffff & c;
                    if (ci < 128) {
                        // nothing special only 7 Bit
                        sb.append(c);
                    } else {
                        // Not 7 Bit use the unicode system
                        sb.append("&#");
                        sb.append(Integer.toString(ci));
                        sb.append(';');
                        }
                    }
                }
            }
        return sb.toString();
    }

    public static String simpleEscapeHtml(String string) {
         StringBuilder sb = new StringBuilder(string.length());
            int len = string.length();

            char c;
            for (int i = 0; i < len; i++)
               {

                switch (c = string.charAt(i)) {
                    case '<':
                        sb.append("&lt;");
                        break;
                    case '>':
                        sb.append("&gt;");
                        break;
                    default:
                       sb.append(c);
                        break;
                }
            }
            return sb.toString();
    }



    public static String norm(final String str) {
        return null == str ? null : str.toLowerCase();
    }

    public static String normAndTrim(final String str) {
        return null == str ? null : str.trim().toLowerCase();
    }

    public static String last(final String str) {
        final StringTokenizer st = new StringTokenizer( str );
        String last = null;
        while ( st.hasMoreTokens() ) {
            last = st.nextToken();
        }
        return last;
    }



    public static String readFully(final URL url) throws IOException {
        final InputStream is = url.openStream();
        try {
            return readFully( is );
        }
        finally {
            is.close();
        }
    }

    public static String readFully(final InputStream is) throws IOException {
        final BufferedReader r = new BufferedReader( new InputStreamReader( is ) );
        final StringBuilder buffer = new StringBuilder();
        for ( int c; 0 <= ( c = r.read() ); ) {
            buffer.append( (char) c );
        }
        return buffer.toString();
    }

    /**
     * The \p{Z} character class represents all standard Unicode whitespace characters.
     * @link http://www.regular-expressions.info/unicode.html#prop
     */
    public static final String UNICODE_WHITESPACE = "\\p{Z}\\s";

    public static String escapeRegexUnicode(final String regex, final boolean useWildCards, final boolean subWhiteSpace){
        final StringBuilder buffer = new StringBuilder( 5 * regex.length() );

        boolean skipWild = false;
        boolean skipWhite = false;
        for ( int i = 0, length = regex.length(); i < length; i++ ) {
            final char c = regex.charAt( i );
            if(useWildCards && c == '*'){
                // substitute wild cards with regex pattern
                if ( !skipWild ) {
                    buffer.append( "[^" + UNICODE_WHITESPACE + "]*" );
                }
                skipWild = true;
                skipWhite = false;
            } else if ( subWhiteSpace && Pattern.matches( "[" + UNICODE_WHITESPACE + "]",  "" + c )  ) {
                // substitute white space character with the regex pattern
                if ( !skipWhite ) {
                    buffer.append( "[" + UNICODE_WHITESPACE + "]+" );
                }
                skipWild = false;
                skipWhite = true;
            } else {
                // encode in hex unicode
                buffer.append(toUnicodeHex(c));
                skipWild = false;
                skipWhite = false;
            }
        }
        return buffer.toString();
    }

    public static String toUnicodeHex(final String s){
        final StringBuilder buffer = new StringBuilder( 5 * s.length() );
        for(int i = 0; i < s.length(); i++){
            buffer.append(toUnicodeHex(s.charAt( i )));
        }
        return buffer.toString();
    }

    public static String toUnicodeHex(final Character c){
        if (c == null) {
            throw new IllegalArgumentException("The passed in Character must not be null");
        }

        final String hexString = Integer.toString(c, 16);
        final String zeros = "000";
        return "\\u" + zeros.substring( 0, 4-hexString.length() ) + hexString;
    }

    public static String escapeRegex(final String regex) {
        final StringBuilder buffer = new StringBuilder( 3 * regex.length() );
        for ( int i = 0, length = regex.length(); i < length; i++ ) {
            final char c = regex.charAt( i );
            // Transform to hex (radix 16):
            final String hexString = Integer.toString(c, 16);
            buffer.append( "\\x" );
            for ( int j = 0; j < 2 - hexString.length(); j++ ) {
                buffer.append( "0" );
            }
            buffer.append( hexString );
        }
        return buffer.toString();
    }

    private static Pattern ESCAPED_HEX_PATTERN = Pattern.compile( "\\\\x([0-9a-fA-F]+)" );

    public static String unescapeRegex(final String regex) {
        final StringBuilder buffer = new StringBuilder( regex.length() / 3 );

        int i = 0;
        final Matcher matcher = ESCAPED_HEX_PATTERN.matcher( regex );
        while ( matcher.find() ) {
            final int
                offset0 = matcher.start(),
                offset1 = matcher.end();
            if ( i < offset0 ) {
                buffer.append( regex.subSequence( i, offset0 ) );
            }


            int charCode = -1;
            try {
                // Parse from hex (radix 16):
                charCode = Integer.parseInt( matcher.group( 1 ), 16 );
            }
            catch ( NumberFormatException e ) {
                log.warn( "Invalid escape sequence; will ignore. [ " + matcher.group( 0 ) + " ]" );
            }

            if ( 0 <= charCode ) {
                buffer.append( (char) charCode );
            }


            i = offset1;
        }

        return buffer.toString();
    }

    /**
     * Removes all UTF control characters from a the passed String,
     * returning a new String.
     *
     * See <a href='http://www.w3.org/TR/2006/REC-xml11-20060816/#charsets'>XML 1.1</a>
     * for more information.
     *
     * @param utf8String
     */
    public static String cleanUTF8String(String utf8String) {
        if (utf8String == null) {
            return null;
        }

        CharBuffer buff = CharBuffer.allocate(utf8String.length());
        buff.append(utf8String).rewind();

        for (int i = 0; i < buff.length(); i++) {
            int charvalue = buff.charAt(i);
            if (isVerbotenCharacter(charvalue)) {
                // replace with a space
                buff.put(i, ' ');
            }
        }
        return buff.toString();
    }

    /**  From http://www.w3.org/TR/2006/REC-xml11-20060816/#charsets
            [#x1-#x8], [#xB-#xC], [#xE-#x1F], [#x7F-#x84], [#x86-#x9F], [#xFDD0-#xFDDF],
            [#x1FFFE-#x1FFFF], [#x2FFFE-#x2FFFF], [#x3FFFE-#x3FFFF],
            [#x4FFFE-#x4FFFF], [#x5FFFE-#x5FFFF], [#x6FFFE-#x6FFFF],
            [#x7FFFE-#x7FFFF], [#x8FFFE-#x8FFFF], [#x9FFFE-#x9FFFF],
            [#xAFFFE-#xAFFFF], [#xBFFFE-#xBFFFF], [#xCFFFE-#xCFFFF],
            [#xDFFFE-#xDFFFF], [#xEFFFE-#xEFFFF], [#xFFFFE-#xFFFFF],
            [#x10FFFE-#x10FFFF].
     */
    private static boolean isVerbotenCharacter(int charvalue) {
        return charvalue >= 0x1 && charvalue <= 0x8 ||
                charvalue >= 0xB && charvalue <= 0xC ||
                charvalue >= 0xE && charvalue <= 0x1F ||
                charvalue >= 0x7F && charvalue <= 0x84 ||
                charvalue >= 0x86 && charvalue <= 0x9F ||
                charvalue >= 0xFDD0 && charvalue <= 0xFDDF ||
                charvalue >= 0xFFFE && charvalue <= 0xFFFF || // FFFE & FFFF are not valid in xml, see above link, QA-43296
                charvalue >= 0x1FFFE && charvalue <= 0x1FFFF ||
                charvalue >= 0x2FFFE && charvalue <= 0x2FFFF ||
                charvalue >= 0x3FFFE && charvalue <= 0x3FFFF ||
                charvalue >= 0x4FFFE && charvalue <= 0x4FFFF ||
                charvalue >= 0x5FFFE && charvalue <= 0x5FFFF ||
                charvalue >= 0x6FFFE && charvalue <= 0x6FFFF ||
                charvalue >= 0x7FFFE && charvalue <= 0x7FFFF ||
                charvalue >= 0x8FFFE && charvalue <= 0x8FFFF ||
                charvalue >= 0x9FFFE && charvalue <= 0x9FFFF ||
                charvalue >= 0xAFFFE && charvalue <= 0xAFFFF ||
                charvalue >= 0xBFFFE && charvalue <= 0xBFFFF ||
                charvalue >= 0xCFFFE && charvalue <= 0xCFFFF ||
                charvalue >= 0xDFFFE && charvalue <= 0xDFFFF ||
                charvalue >= 0xEFFFE && charvalue <= 0xEFFFF ||
                charvalue >= 0xFFFFE && charvalue <= 0xFFFFF ||
                charvalue >= 0x10FFFE && charvalue <= 0x10FFFF;
    }

    /** QA-85036
     * Removes all UTF control characters from a the passed String,
     * returning a new String EXCEPT a &amp#xD; which is equivalent to \r.
     *
     * @see #cleanUTF8String(String)
     */
    public static String cleanUTF8StringWithPrejudice(String utf8String) {

        if (utf8String == null) {
            return null;
        }
        CharBuffer buff = CharBuffer.allocate(utf8String.length());
        buff.append(utf8String).rewind();

        for (int i = 0; i < buff.length(); i++) {
            int charvalue = buff.charAt(i);
            if (charvalue == 0xD) {
                throw new PalantirXmlException("Unicode character &#xD; is not allowed in pXML.");
            } else if (isVerbotenCharacter(charvalue)) {
                buff.put(i, ' '); // replace with a space
            }
        }
        return buff.toString();
    }

    public static boolean isEmpty(final String text) {
        return null == text || text.trim().length() <= 0;
    }

    public static interface NameSpace {
        public boolean contains(final String name);
    }

    public static class SetNameSpace implements NameSpace {
        private Set<String> normTrimSet;

        public SetNameSpace(final Set<String> names) {
            if (names != null && names.size() > 0) {
                normTrimSet = new HashSet<String>( names.size() );
                for ( String name : names ) {
                    normTrimSet.add( normAndTrim( name ) );
                }
            } else {
                normTrimSet = Collections.emptySet();
            }
        }

        // =========================
        // <code>NameSpace</code> implementation

        @Override
        public boolean contains(final String name) {
            return normTrimSet.contains( normAndTrim( name ) );
        }

        // =========================
    }

    public static long hashString(final String externalKey) {
        if (null == externalKey) {
            return 0;
        }
        CRC32 crc = new CRC32();
        try {
            crc.update(externalKey.getBytes(TextUtils.CHARSET_UTF_8));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 must be supported", e);
        }
        return crc.getValue();
    }

    /**
     * Converts the string to bytes using the UTF-8 encoding. The string can be
     * decoded using {@link #convertBytesToStringUtf8(byte[])}.
     *
     * @param value the string to convert.
     * @return the encoded bytes.
     */
    public static byte[] convertStringToBytesUtf8(String value) {
        try {
            return value.getBytes(TextUtils.CHARSET_UTF_8);
        } catch (UnsupportedEncodingException e) {
            // This should never happen.
            throw new IllegalStateException("Cannot encode using UTF-8.");
        }
    }

    /**
     * Converts the bytes into a string assuming the bytes have the UTF-8
     * encoding.
     *
     * @param bytes the bytes to convert.
     * @return the decoded string.
     */
    public static String convertBytesToStringUtf8(byte[] bytes) {
        try {
            return new String(bytes, TextUtils.CHARSET_UTF_8);
        } catch (UnsupportedEncodingException e) {
            // This should never happen.
            throw new IllegalStateException("Cannot decode using UTF-8.");
        }
    }

    /**
     * [1e0, 1e4) -> displayed as number
     * [1e4, 1e7) -> displayed as number of thousands
     * [1e7, 1e10) -> displayed as number of millions
     * [1e10, 1e12) -> displayed as number of billions
     * [1e12, 1e15) -> displayed as number of trillions
     * [1e15, infin) -> displayed in scientific notation
     * @return
     */
    public static String getStringForValue(double val) {
        return getStringForValue(val, false);
    }

    public static String getStringForValue(double val, boolean shortDisplay) {
        if (val == 0f) {
            return "0";
        }
        if (Double.isNaN(val)) {
            return "";
        }
        String neg = "";
        if (val < 0) {
            val = Math.abs(val);
            neg = "-";
        }
        int pow = (int)Math.ceil(Math.log10(val));
        if (Math.log10(val) == Math.ceil(Math.log10(val)))
         {
            pow++; //behave nicely on the power of 10 breaks
        }
        if (pow < 0) {
            return "";
        }

        if (!shortDisplay) {
            switch (pow){
                case 15: return String.format("%s%.0fT", neg, val / 1e12);
                case 14: return String.format("%s%.0fT", neg, val / 1e12);
                case 13: return String.format("%s%.1fT", neg, val / 1e12);
                case 12: return String.format("%s%.0fB", neg, val / 1e9);
                case 11: return String.format("%s%.0fB", neg, val / 1e9);
                case 10: return String.format("%s%.1fB", neg, val / 1e9);
                case 9:  return String.format("%s%.0fM", neg, val / 1e6);
                case 8:  return String.format("%s%.0fM", neg, val / 1e6);
                case 7:  return String.format("%s%.1fM", neg, val / 1e6);
                case 6:  return String.format("%s%.0fk", neg, val / 1e3);
                case 5:  return String.format("%s%.0fk", neg, val / 1e3);
                case 4:  return String.format("%s%.1fk", neg, val / 1e3);
                case 3:  return String.format("%s%.0f", neg, val);
                case 2:  return String.format("%s%.0f", neg, val);
                case 1:  return String.format("%s%.0f", neg, val);
                case 0:  return String.format("%s%.0f", neg, val);
                default: return String.format("%s%.1e", neg, val);
            }
        }
        switch (pow){
            case 15: return String.format("%s%.1fT", neg, val / 1e12);
            case 14: return String.format("%s%.1fT", neg, val / 1e12);
            case 13: return String.format("%s%.2fT", neg, val / 1e12);
            case 12: return String.format("%s%.1fB", neg, val / 1e9);
            case 11: return String.format("%s%.1fB", neg, val / 1e9);
            case 10: return String.format("%s%.2fB", neg, val / 1e9);
            case 9:  return String.format("%s%.1fM", neg, val / 1e6);
            case 8:  return String.format("%s%.1fM", neg, val / 1e6);
            case 7:  return String.format("%s%.2fM", neg, val / 1e6);
            case 6:  return String.format("%s%.1fk", neg, val / 1e3);
            case 5:  return String.format("%s%.1fk", neg, val / 1e3);
            case 4:  return String.format("%s%.2fk", neg, val / 1e3);
            case 3:  return String.format("%s%.1f", neg, val);
            case 2:  return String.format("%s%.1f", neg, val);
            case 1:  return String.format("%s%.2f", neg, val);
            case 0:  return String.format("%s%.3f", neg, val);
            default: return String.format("%s%.2e", neg, val);
        }
    }

    /**
     * Formats the text, inserting line breaks as necessary.  Wraps around
     * format.
     *
     * @param f Font to use in calculation
     * @param text Text to format
     * @param maxWidth Maximum width of text
     * @return Formatted text
     */
    public static String htmlFormat(Font f, String text, int maxWidth) {
        return "<HTML>" + format(f, text, maxWidth, "<br>");
    }

    /**
     * Formats the text, inserting line breaks as necessary.
     *
     * @param f Font to use in calculation
     * @param text Text to format
     * @param maxWidth Maximum width of text
     * @param delim End of line delimiter
     * @return Formatted text
     */
    public static String format(Font f, String text, int maxWidth, String delim) {
        FontMetrics fm = Toolkit.getDefaultToolkit().getFontMetrics(f);
        StringTokenizer st = new StringTokenizer(text, " \t\n\r\f", true);
        StringBuilder all = new StringBuilder();
        StringBuilder curLine = new StringBuilder();
        while (st.hasMoreTokens()) {
            String tk = st.nextToken();
            append(fm, all, curLine, tk, maxWidth, delim);
            if (SwingUtilities.computeStringWidth(fm, curLine.toString()) > maxWidth) {
                curLine = truncate(fm, all, tk, maxWidth, delim);
            }
        }
        return all.toString() + curLine.toString();
    }

    /**
     * If any word is too long to fit on one line, we truncate it here.
     * Return the curLine object holding the last line of the long word.
     *
     * @param fm Font metrics for measurement.
     * @param all All the text.
     * @param tk Token we are attempting to add.
     * @param maxWidth Maximum width of text.
     * @param delim End of line delimiter
     * @return Last bit of the long word.
     */
    private static StringBuilder truncate(FontMetrics fm, StringBuilder all, String tk, int maxWidth, String delim) {
        StringBuilder curLine = new StringBuilder();
        for (int i = 0; i < tk.length(); i++) {
            append(fm, all, curLine, "" + tk.charAt(i), maxWidth, delim);
        }
        return curLine;
    }

    /**
     * Attempts to append token to the current line.  If this makes the current
     * line too long, we append the current line to everything and restart the
     * current line.
     *
     * @param fm Font metrics for measurement.
     * @param all All the text.
     * @param curLine The current line of text.
     * @param tk Token we are attempting to add.
     * @param maxWidth Maximum width of text.
     * @param delim End of line delimiter
     */
    private static void append(FontMetrics fm, StringBuilder all, StringBuilder curLine, String tk, int maxWidth, String delim) {
        String soFar = curLine.toString();
        curLine.append(tk);
        int width = SwingUtilities.computeStringWidth(fm, curLine.toString());
        if (width > maxWidth) {
            if (soFar.trim().length() > 0) {
                all.append(soFar + delim);
            }
            curLine.setLength(0);
            curLine.append(tk);
        }
    }
}
