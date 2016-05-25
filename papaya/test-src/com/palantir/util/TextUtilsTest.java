package com.palantir.util;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class TextUtilsTest extends Assert
{
    //A single unicode char that takes up 4 bytes in UTF-16 and 4 bytes in UTF-8
    public static final String s3 = "𝐀";

    public TextUtilsTest(){/**/}

    @Test
    public void testProperCaseWord() throws Exception {
        String[] words = new String[] { "AA102", "nw", "dog", "daVID CHiu", "yu-gi-oh rules" };
        String[] results = new String[] { "AA102", "Nw", "Dog", "David chiu", "Yu-gi-oh rules" };
        for (int i=0; i < words.length; i++) {
            String result = TextUtils.properCaseWord(words[i]);
            assertEquals(results[i], result);
        }
    }

    @Test
    public void testProperCaseWords() throws Exception {
        String[] words = new String[] { "AA102", "nw", "dog", "daVID CHiu", "yu-gi-oh rules",
                "b.j. penn the great,shawn sherk"};
        String[] results = new String[] { "AA102", "Nw", "Dog", "David Chiu", "Yu-Gi-Oh Rules",
                "B.J. Penn The Great,Shawn Sherk"};
        for (int i=0; i < words.length; i++) {
            String result = TextUtils.properCaseWords(words[i]);
            assertEquals(results[i], result);
        }
    }

    @Test
    public void testPluralization() throws Exception {
        assertEquals("", TextUtils.pluralize(null));
        assertEquals("", TextUtils.pluralize(""));
        assertEquals("dogs", TextUtils.pluralize("dog"));
        assertEquals("keywords", TextUtils.pluralize("keywords"));
    }

    @Test
    public void testStringForValue() throws Exception {
        assertEquals("0", TextUtils.getStringForValue(0.0));
        assertEquals("5", TextUtils.getStringForValue(5.0));
        assertEquals("100", TextUtils.getStringForValue(100.0));
        assertEquals("9.2k", TextUtils.getStringForValue(9204.0));
        assertEquals("9.5k", TextUtils.getStringForValue(9499.0));
        assertEquals("10.0k", TextUtils.getStringForValue(9999.0));
        assertEquals("100k", TextUtils.getStringForValue(99999.0));
        assertEquals("100k", TextUtils.getStringForValue(100000.0));
        assertEquals("1.0M", TextUtils.getStringForValue(1000000.0));
        assertEquals("1.5M", TextUtils.getStringForValue(1499999.0));
        assertEquals("10M", TextUtils.getStringForValue(10000000.0));
        assertEquals("15M", TextUtils.getStringForValue(14999999.0));
        assertEquals("100M", TextUtils.getStringForValue(100000000.0));
        assertEquals("150M", TextUtils.getStringForValue(149999990.0));
        assertEquals("1.0B", TextUtils.getStringForValue(1000000000.0));
        assertEquals("1.5B", TextUtils.getStringForValue(1499999900.0));
        assertEquals("10B", TextUtils.getStringForValue(10000000000.0));
        assertEquals("15B", TextUtils.getStringForValue(14999999000.0));
        assertEquals("100B", TextUtils.getStringForValue(100000000000.0));
        assertEquals("150B", TextUtils.getStringForValue(149999990000.0));
        assertEquals("1.0T", TextUtils.getStringForValue(1000000000000.0));
        assertEquals("1.5T", TextUtils.getStringForValue(1499999900000.0));
        assertEquals("10T", TextUtils.getStringForValue(10000000000000.0));
        assertEquals("15T", TextUtils.getStringForValue(14999999000000.0));
        assertEquals("100T", TextUtils.getStringForValue(100000000000000.0));
        assertEquals("150T", TextUtils.getStringForValue(149999990000000.0));
        assertEquals("1.0e+15", TextUtils.getStringForValue(1000000000000000.0));
        assertEquals("1.5e+15", TextUtils.getStringForValue(1499999900000000.0));

        assertEquals("-5", TextUtils.getStringForValue(-5.0));
        assertEquals("-100", TextUtils.getStringForValue(-100.0));
        assertEquals("-9.2k", TextUtils.getStringForValue(-9204.0));
        assertEquals("-9.5k", TextUtils.getStringForValue(-9499.0));
        assertEquals("-10.0k", TextUtils.getStringForValue(-9999.0));
        assertEquals("-100k", TextUtils.getStringForValue(-99999.0));
        assertEquals("-100k", TextUtils.getStringForValue(-100000.0));
        assertEquals("-1.0M", TextUtils.getStringForValue(-1000000.0));
        assertEquals("-1.5M", TextUtils.getStringForValue(-1499999.0));
        assertEquals("-10M", TextUtils.getStringForValue(-10000000.0));
        assertEquals("-15M", TextUtils.getStringForValue(-14999999.0));
        assertEquals("-100M", TextUtils.getStringForValue(-100000000.0));
        assertEquals("-150M", TextUtils.getStringForValue(-149999990.0));
        assertEquals("-1.0B", TextUtils.getStringForValue(-1000000000.0));
        assertEquals("-1.5B", TextUtils.getStringForValue(-1499999900.0));
        assertEquals("-1.5B", TextUtils.getStringForValue(-1500000001.0));
        assertEquals("-10B", TextUtils.getStringForValue(-10000000000.0));
        assertEquals("-15B", TextUtils.getStringForValue(-14999999000.0));
        assertEquals("-100B", TextUtils.getStringForValue(-100000000000.0));
        assertEquals("-150B", TextUtils.getStringForValue(-149999990000.0));
        assertEquals("-1.0T", TextUtils.getStringForValue(-1000000000000.0));
        assertEquals("-1.5T", TextUtils.getStringForValue(-1499999900000.0));
        assertEquals("-10T", TextUtils.getStringForValue(-10000000000000.0));
        assertEquals("-15T", TextUtils.getStringForValue(-14999999000000.0));
        assertEquals("-100T", TextUtils.getStringForValue(-100000000000000.0));
        assertEquals("-150T", TextUtils.getStringForValue(-149999990000000.0));
        assertEquals("-1.0e+15", TextUtils.getStringForValue(-1000000000000000.0));
        assertEquals("-1.5e+15", TextUtils.getStringForValue(-1499999900000000.0));
    }

    @Test
    public void testRemoveAllWhitespace() {
        String before = "  \r\n\n\r  \t FOOOooo\r\n\n\n\r\t\r o   \n";
        String after = TextUtils.removeAllWhitespace(before);
        assertEquals("FOOOoooo", after);
    }

    @Test
    public void testTruncateStringToCharLength() {
        String string = "abcde";
        assertEquals(string, TextUtils.truncateStringToCharLength(string, 5, "..."));
        assertEquals(string, TextUtils.truncateStringToCharLength(string, 5, ""));
        assertEquals(string, TextUtils.truncateStringToCharLength(string, 6, "..."));
        assertEquals(string, TextUtils.truncateStringToCharLength(string, 6, ""));
        assertEquals("a...", TextUtils.truncateStringToCharLength(string, 4, "..."));
        assertEquals("abcd", TextUtils.truncateStringToCharLength(string, 4, ""));
    }

    @Test
    public void testTruncateLabelString() {
        // TODO(nackner): Add in more tests with UTF-8 characters, but they won't play nice with the
        // build or people's Eclipse clients even if commented out.
        String string = "abcde";
        assertEquals(string, TextUtils.truncateLabelString(string, 5));
        assertEquals(string, TextUtils.truncateLabelString(string, 6, "..."));
        assertEquals("a...", TextUtils.truncateLabelString(string, 4, "..."));
    }

    @Test
    public void testPropertiesToXML()
    {
        // simple string kv pair
        Properties p = new Properties();
        p.setProperty("MY_CONFIG_KEY", "MY_CONFIG_VALUE");
        String propertiesAsXML = TextUtils.storePropertiesToXMLString(p);
        assertNotNull(propertiesAsXML);
        p = TextUtils.loadPropertiesFromXMLString(propertiesAsXML);
        assertNotNull(p.getProperty("MY_CONFIG_KEY"));
        assertEquals("MY_CONFIG_VALUE", p.getProperty("MY_CONFIG_KEY"));

        // embedded config
        Properties pComplex = new Properties();
        pComplex.setProperty("MY_SUB_CONFIG", TextUtils.storePropertiesToXMLString(p));
        propertiesAsXML = TextUtils.storePropertiesToXMLString(pComplex);
        assertNotNull(propertiesAsXML);
        pComplex = TextUtils.loadPropertiesFromXMLString(propertiesAsXML);
        p = TextUtils.loadPropertiesFromXMLString(pComplex.getProperty("MY_SUB_CONFIG"));
        assertNotNull(p.getProperty("MY_CONFIG_KEY"));
        assertEquals("MY_CONFIG_VALUE", p.getProperty("MY_CONFIG_KEY"));
    }

    @Test
    public void testMaximalPrefix() throws Exception{
        ArrayList<String> list1 = new ArrayList<String>(7);
        list1.add("abcdef");
        list1.add("abcdefg");
        list1.add("abcdefgh");
        list1.add("abcd");
        list1.add("abcdefl58a");
        list1.add("abcdeeeeee");
        list1.add("abcde888");
        assertEquals("Wrong maximal prefix","abcd",TextUtils.findMaximalPrefix(list1));

        list1.clear();
        assertEquals("Should be empty string","",TextUtils.findMaximalPrefix(list1));
        assertEquals("Should be empty string","",TextUtils.findMaximalPrefix(null));

        list1.add("abcd");
        assertEquals("Should be abcd","abcd",TextUtils.findMaximalPrefix(list1));
        list1.add("efgh");
        list1.add("ifht");

        assertEquals("Should be empty string","",TextUtils.findMaximalPrefix(list1));
    }

    @Test
    public void testHexConverter(){
        byte[] bytes = new byte[]{(byte)255, (byte)255, 0, 0};
        System.out.println(Arrays.toString(bytes) +" -> 0x" + TextUtils.byteArrayToHexString(bytes));
        assertEquals("ffff0000", TextUtils.byteArrayToHexString(bytes));
    }

    @Test
    public void testParseDate()
    {
        helperTestParseDate(new Date());
    }

    @Test
    public void testParseDateFeb29() {
        helperTestParseDate(new Date(2012 - 1900, 1, 29));
    }

    private void helperTestParseDate(Date testDate) {
        for(int i : getFormatterIndicesToTest(testDate))
        {
            // convert the seed Date to a string and re-parse
            ParsePosition pos = new ParsePosition(0);
            String formattedString = getDateFormatter(i).format(testDate);
            Date parsedDate = getDateFormatter(i).parse(formattedString, pos);

            // ensure exact match
            if(pos.getIndex() != formattedString.length()) {
                parsedDate = null;
            }

            // do again to ensure
            pos = new ParsePosition(0);
            String formattedString2 = getDateFormatter(i).format(parsedDate);
            Date parsedDate2 = getDateFormatter(i).parse(formattedString2, pos);
            if(pos.getIndex() != formattedString2.length()) {
                parsedDate2 = null;
            }

            // we should now have two comparable values
            assertEquals(formattedString, formattedString2);
            assertEquals(parsedDate, parsedDate2);
        }
    }

    private int[] getFormatterIndicesToTest(Date date) {
        if (date.getMonth() == 1 && date.getDate() == 29) {
            // For Feb 29, skip patterns that have month and day without year
            return new int[] {1,3,4,5,6,7,8,9,10,11,12,13,14,15};
        } else {
            // there are currently 15 formats in the TextUtils class, try them all
            return new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
        }
    }

    private static DateFormat getDateFormatter(int format)
    {
        switch(format) {
            case 1:
                return new SimpleDateFormat("yyyy");
            case 2:
                return new SimpleDateFormat("MMMMM dd");
            case 3:
                return new SimpleDateFormat("MMMMM");
            case 4:
                return new SimpleDateFormat("HH:mm");
            case 5:
                return new SimpleDateFormat("HH:mm:ss");
            case 6:
                return new SimpleDateFormat("MMMMMM dd, yyyy HH:mm");
            case 7:
                return new SimpleDateFormat("MMMMM dd, yyyy hh:mm aaa");
            case 8:
                return new SimpleDateFormat("MM/dd/yyyy HH:mm z");
            case 9:
                return new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z");
            case 10:
                return new SimpleDateFormat("MMMMM dd, yyyy");
            case 11:
                return new SimpleDateFormat("MM/dd/yyyy HH:mm");
            case 12:
                return new SimpleDateFormat("HH:mm MM/dd/yy");
            case 13:
                return new SimpleDateFormat("yyyy/MM");
            case 14:
                return new SimpleDateFormat("MM/dd/yyyy");
            default:
                return new SimpleDateFormat("MM/dd/yy");
        }
    }

    @Test
    public void testcleanUTF8String() throws Exception {
        String cleanString = "Hello World";
        String dirtyString = "Hello\u0007World";

        String cleanedString = TextUtils.cleanUTF8String(dirtyString);
        assertEquals(cleanString, cleanedString);
        assertEquals(cleanString, TextUtils.cleanUTF8String(cleanString));
    }

    @Test
    public void testHashString() throws Exception {
        String testStr = null;
        long hash = TextUtils.hashString(testStr);
        assertEquals(0, hash);

        testStr = "Allen cheats at Race for the Galaxy.";
        hash = TextUtils.hashString(testStr);
        assertEquals(1133932183, hash);
    }

    @Test
    public void testEncodeDecodeStringUTF8() throws Exception {
        String str = "THIS IS A \u1234 TEST STRING";
        byte[] bytes = TextUtils.convertStringToBytesUtf8(str);
        assertTrue(Arrays.equals(str.getBytes("UTF-8"), bytes));
        assertEquals(str, TextUtils.convertBytesToStringUtf8(bytes));
    }

    @Test
    public void testEscapeHtmlBasic() {
        String input1 = "\"A\" \"b\"; 1 < 2 && 3 > 2";
        String output1 = "&quot;A&quot; &quot;b&quot;; 1 &lt; 2 &amp;&amp; 3 &gt; 2";
        assertTrue(output1.equals(TextUtils.escapeHtml(input1)));
    }

    @Test
    public void testEscapeHtmlWhitespaceHandling() {
        String input2 = "a b  c   d    e";
        String output2 = "a b &nbsp;c &nbsp; d &nbsp; &nbsp;e";
        assertTrue(output2.equals(TextUtils.escapeHtml(input2)));

        String input3 = "line 1\nline 2 \n\n line4";
        String output3f = "line 1<br/>line 2 <br/><br/> line4";
        String output3t = "line 1line 2  line4";
        assertTrue(output3f.equals(TextUtils.escapeHtml(input3, false)));
        assertTrue(output3t.equals(TextUtils.escapeHtml(input3, true)));
    }

    private static final String u00C0 = "À";
    private static final String u0100 = "Ā";
    private static final String u0120 = "Ġ";

    @Test
    public void testEscapeHtmlTwoByteUnicode() {
        assertTrue("&#192;".equals(TextUtils.escapeHtml(u00C0)));
        assertTrue("&#256;".equals(TextUtils.escapeHtml(u0100)));
        assertTrue("&#288;".equals(TextUtils.escapeHtml(u0120)));
    }
}
