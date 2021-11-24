/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.proto.fork;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Forked version of com.googlecode.protobuf.format.JsonFormat from protobuf-java-format-1.2
 * Fork changes the serialization/deserialization of bytestrings to use base64 encoding
 * instead of escaping
 * <p>
 * Provide ascii text parsing and formatting support for proto2 instances. The implementation
 * largely follows google/protobuf/text_format.cc.
 * <p>
 * (c) 2009-10 Orbitz World Wide. All Rights Reserved.
 *
 * @author eliran.bivas@gmail.com Eliran Bivas
 * @author aantonov@orbitz.com Alex Antonov
 *         <p/>
 *         Based on the original code by:
 * @author wenboz@google.com Wenbo Zhu
 * @author kenton@google.com Kenton Varda
 */
public final class ForkedJsonFormat {

    private ForkedJsonFormat() {
        // utility
    }

    /**
     * Outputs a textual representation of the Protocol Message supplied into the parameter output.
     * (This representation is the new version of the classic "ProtocolPrinter" output from the
     * original Protocol Buffer system)
     */
    public static void print(Message message, Appendable output) throws IOException {
        JsonGenerator generator = new JsonGenerator(output);
        generator.print("{");
        print(message, generator);
        generator.print("}");
    }

    /**
     * Outputs a textual representation of {@code fields} to {@code output}.
     */
    public static void print(UnknownFieldSet fields, Appendable output) throws IOException {
        JsonGenerator generator = new JsonGenerator(output);
        generator.print("{");
        printUnknownFields(fields, generator);
        generator.print("}");
    }

    private static void print(Message message, JsonGenerator generator) throws IOException {
        for (Iterator<Map.Entry<FieldDescriptor, Object>> iter =
                        message.getAllFields().entrySet().iterator();
                iter.hasNext(); ) {
            Map.Entry<FieldDescriptor, Object> field = iter.next();
            printField(field.getKey(), field.getValue(), generator);
            if (iter.hasNext()) {
                generator.print(",");
            }
        }
        if (message.getUnknownFields().asMap().size() > 0) {
            generator.print(", ");
        }
        printUnknownFields(message.getUnknownFields(), generator);
    }

    /**
     * Like {@code print()}, but writes directly to a {@code String} and returns it.
     */
    public static String printToString(Message message) {
        try {
            StringBuilder text = new StringBuilder();
            print(message, text);
            return text.toString();
        } catch (IOException e) {
            throw new SafeRuntimeException("Writing to a StringBuilder threw an IOException (should never happen).", e);
        }
    }

    /**
     * Like {@code print()}, but writes directly to a {@code String} and returns it.
     */
    public static String printToString(UnknownFieldSet fields) {
        try {
            StringBuilder text = new StringBuilder();
            print(fields, text);
            return text.toString();
        } catch (IOException e) {
            throw new SafeRuntimeException("Writing to a StringBuilder threw an IOException (should never happen).", e);
        }
    }

    public static void printField(FieldDescriptor field, Object value, JsonGenerator generator) throws IOException {
        printSingleField(field, value, generator);
    }

    private static void printSingleField(FieldDescriptor field, Object value, JsonGenerator generator)
            throws IOException {
        if (field.isExtension()) {
            generator.print("\"");
            // We special-case MessageSet elements for compatibility with proto1.
            if (field.getContainingType().getOptions().getMessageSetWireFormat()
                    && (field.getType() == FieldDescriptor.Type.MESSAGE)
                    && field.isOptional()
                    // object equality
                    && (field.getExtensionScope() == field.getMessageType())) {
                generator.print(field.getMessageType().getFullName());
            } else {
                generator.print(field.getFullName());
            }
            generator.print("\"");
        } else {
            generator.print("\"");
            if (field.getType() == FieldDescriptor.Type.GROUP) {
                // Groups must be serialized with their original capitalization.
                generator.print(field.getMessageType().getName());
            } else {
                generator.print(field.getName());
            }
            generator.print("\"");
        }

        // Done with the name, on to the value

        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            generator.print(": ");
            generator.indent();
        } else {
            generator.print(": ");
        }

        if (field.isRepeated()) {
            // Repeated field. Print each element.
            generator.print("[");
            for (Iterator<?> iter = ((List<?>) value).iterator(); iter.hasNext(); ) {
                printFieldValue(field, iter.next(), generator);
                if (iter.hasNext()) {
                    generator.print(",");
                }
            }
            generator.print("]");
        } else {
            printFieldValue(field, value, generator);
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                generator.outdent();
            }
        }
    }

    private static void printFieldValue(FieldDescriptor field, Object value, JsonGenerator generator)
            throws IOException {
        switch (field.getType()) {
            case INT32:
            case INT64:
            case SINT32:
            case SINT64:
            case SFIXED32:
            case SFIXED64:
            case FLOAT:
            case DOUBLE:
            case BOOL:
                // Good old toString() does what we want for these types.
                generator.print(value.toString());
                break;

            case UINT32:
            case FIXED32:
                generator.print(unsignedToString((Integer) value));
                break;

            case UINT64:
            case FIXED64:
                generator.print(unsignedToString((Long) value));
                break;

            case STRING:
                generator.print("\"");
                generator.print(escapeText((String) value));
                generator.print("\"");
                break;

            case BYTES: {
                generator.print("\"");
                generator.print(escapeBytes((ByteString) value));
                generator.print("\"");
                break;
            }

            case ENUM: {
                generator.print("\"");
                generator.print(((EnumValueDescriptor) value).getName());
                generator.print("\"");
                break;
            }

            case MESSAGE:
            case GROUP:
                generator.print("{");
                print((Message) value, generator);
                generator.print("}");
                break;

            default:
        }
    }

    private static void printUnknownFields(UnknownFieldSet unknownFields, JsonGenerator generator) throws IOException {
        boolean firstField = true;
        for (Map.Entry<Integer, UnknownFieldSet.Field> entry :
                unknownFields.asMap().entrySet()) {
            UnknownFieldSet.Field field = entry.getValue();

            if (firstField) {
                firstField = false;
            } else {
                generator.print(", ");
            }

            generator.print("\"");
            generator.print(entry.getKey().toString());
            generator.print("\"");
            generator.print(": [");

            boolean firstValue = true;
            for (long value : field.getVarintList()) {
                if (firstValue) {
                    firstValue = false;
                } else {
                    generator.print(", ");
                }
                generator.print(unsignedToString(value));
            }
            for (int value : field.getFixed32List()) {
                if (firstValue) {
                    firstValue = false;
                } else {
                    generator.print(", ");
                }
                generator.print(String.format((Locale) null, "0x%08x", value));
            }
            for (long value : field.getFixed64List()) {
                if (firstValue) {
                    firstValue = false;
                } else {
                    generator.print(", ");
                }
                generator.print(String.format((Locale) null, "0x%016x", value));
            }
            for (ByteString value : field.getLengthDelimitedList()) {
                if (firstValue) {
                    firstValue = false;
                } else {
                    generator.print(", ");
                }
                generator.print("\"");
                generator.print(escapeBytes(value));
                generator.print("\"");
            }
            for (UnknownFieldSet value : field.getGroupList()) {
                if (firstValue) {
                    firstValue = false;
                } else {
                    generator.print(", ");
                }
                generator.print("{");
                printUnknownFields(value, generator);
                generator.print("}");
            }
            generator.print("]");
        }
    }

    /**
     * Convert an unsigned 32-bit integer to a string.
     */
    private static String unsignedToString(int value) {
        if (value >= 0) {
            return Integer.toString(value);
        } else {
            return Long.toString(value & 0x00000000FFFFFFFFL);
        }
    }

    /**
     * Convert an unsigned 64-bit integer to a string.
     */
    private static String unsignedToString(long value) {
        if (value >= 0) {
            return Long.toString(value);
        } else {
            // Pull off the most-significant bit so that BigInteger doesn't think
            // the number is negative, then set it again using setBit().
            return BigInteger.valueOf(value & 0x7FFFFFFFFFFFFFFFL).setBit(63).toString();
        }
    }

    /**
     * An inner class for writing text to the output stream.
     */
    protected static class JsonGenerator {

        Appendable output;
        boolean atStartOfLine = true;
        StringBuilder indent = new StringBuilder();

        public JsonGenerator(Appendable output) {
            this.output = output;
        }

        /**
         * Indent text by two spaces. After calling Indent(), two spaces will be inserted at the
         * beginning of each line of text. Indent() may be called multiple times to produce deeper
         * indents.
         */
        public void indent() {
            indent.append("  ");
        }

        /**
         * Reduces the current indent level by two spaces, or crashes if the indent level is zero.
         */
        public void outdent() {
            int length = indent.length();
            if (length == 0) {
                throw new SafeIllegalArgumentException(" Outdent() without matching Indent().");
            }
            indent.delete(length - 2, length);
        }

        /**
         * Print text to the output stream.
         */
        public void print(CharSequence text) throws IOException {
            int size = text.length();
            int pos = 0;

            for (int i = 0; i < size; i++) {
                if (text.charAt(i) == '\n') {
                    write(text.subSequence(pos, size), i - pos + 1);
                    pos = i + 1;
                    atStartOfLine = true;
                }
            }
            write(text.subSequence(pos, size), size - pos);
        }

        private void write(CharSequence data, int size) throws IOException {
            if (size == 0) {
                return;
            }
            if (atStartOfLine) {
                atStartOfLine = false;
                output.append(indent);
            }
            output.append(data);
        }
    }

    // =================================================================
    // Parsing

    /**
     * Represents a stream of tokens parsed from a {@code String}.
     * <p/>
     * <p>
     * The Java standard library provides many classes that you might think would be useful for
     * implementing this, but aren't. For example:
     * <p/>
     * <ul>
     * <li>{@code java.io.StreamTokenizer}: This almost does what we want -- or, at least, something
     * that would get us close to what we want -- except for one fatal flaw: It automatically
     * un-escapes strings using Java escape sequences, which do not include all the escape sequences
     * we need to support (e.g. '\x').
     * <li>{@code java.util.Scanner}: This seems like a great way at least to parse regular
     * expressions out of a stream (so we wouldn't have to load the entire input into a single
     * string before parsing). Sadly, {@code Scanner} requires that tokens be delimited with some
     * delimiter. Thus, although the text "foo:" should parse to two tokens ("foo" and ":"), {@code
     * Scanner} would recognize it only as a single token. Furthermore, {@code Scanner} provides no
     * way to inspect the contents of delimiters, making it impossible to keep track of line and
     * column numbers.
     * </ul>
     * <p/>
     * <p>
     * Luckily, Java's regular expression support does manage to be useful to us. (Barely: We need
     * {@code Matcher.usePattern()}, which is new in Java 1.5.) So, we can use that, at least.
     * Unfortunately, this implies that we need to have the entire input in one contiguous string.
     */
    protected static class Tokenizer {

        private final CharSequence text;
        private final Matcher matcher;
        private String currentToken;

        // The character index within this.text at which the current token begins.
        private int pos = 0;

        // The line and column numbers of the current token.
        private int line = 0;
        private int column = 0;

        // The line and column numbers of the previous token (allows throwing
        // errors *after* consuming).
        private int previousLine = 0;
        private int previousColumn = 0;

        // We use possesive quantifiers (*+ and ++) because otherwise the Java
        // regex matcher has stack overflows on large inputs.
        private static final Pattern WHITESPACE = Pattern.compile("(\\s|(#.*$))++", Pattern.MULTILINE);
        private static final Pattern TOKEN = Pattern.compile(
                "[a-zA-Z_][0-9a-zA-Z_+-]*+|"
                        + // an identifier
                        "[.]?[0-9+-][0-9a-zA-Z_.+-]*+|"
                        + // a number
                        "\"([^\"\n\\\\]|\\\\.)*+(\"|\\\\?$)|"
                        + // a double-quoted string
                        "\'([^\'\n\\\\]|\\\\.)*+(\'|\\\\?$)", // a single-quoted string
                Pattern.MULTILINE);

        private static final Pattern DOUBLE_INFINITY = Pattern.compile("-?inf(inity)?", Pattern.CASE_INSENSITIVE);
        private static final Pattern FLOAT_INFINITY = Pattern.compile("-?inf(inity)?f?", Pattern.CASE_INSENSITIVE);
        private static final Pattern FLOAT_NAN = Pattern.compile("nanf?", Pattern.CASE_INSENSITIVE);

        /**
         * Construct a tokenizer that parses tokens from the given text.
         */
        public Tokenizer(CharSequence text) {
            this.text = text;
            matcher = WHITESPACE.matcher(text);
            skipWhitespace();
            nextToken();
        }

        /*
         * Are we at the end of the input?
         */
        public boolean atEnd() {
            return currentToken.length() == 0;
        }

        /**
         * Advance to the next token.
         */
        public void nextToken() {
            previousLine = line;
            previousColumn = column;

            // Advance the line counter to the current position.
            while (pos < matcher.regionStart()) {
                if (text.charAt(pos) == '\n') {
                    ++line;
                    column = 0;
                } else {
                    ++column;
                }
                ++pos;
            }

            // Match the next token.
            if (matcher.regionStart() == matcher.regionEnd()) {
                // EOF
                currentToken = "";
            } else {
                matcher.usePattern(TOKEN);
                if (matcher.lookingAt()) {
                    currentToken = matcher.group();
                    matcher.region(matcher.end(), matcher.regionEnd());
                } else {
                    // Take one character.
                    currentToken = String.valueOf(text.charAt(pos));
                    matcher.region(pos + 1, matcher.regionEnd());
                }

                skipWhitespace();
            }
        }

        /**
         * Skip over any whitespace so that the matcher region starts at the next token.
         */
        private void skipWhitespace() {
            matcher.usePattern(WHITESPACE);
            if (matcher.lookingAt()) {
                matcher.region(matcher.end(), matcher.regionEnd());
            }
        }

        /**
         * If the next token exactly matches {@code token}, consume it and return {@code true}.
         * Otherwise, return {@code false} without doing anything.
         */
        public boolean tryConsume(String token) {
            if (currentToken.equals(token)) {
                nextToken();
                return true;
            } else {
                return false;
            }
        }

        /**
         * If the next token exactly matches {@code token}, consume it. Otherwise, throw a
         * {@link ParseException}.
         */
        public void consume(String token) throws ParseException {
            if (!tryConsume(token)) {
                throw parseException("Expected \"" + token + "\".");
            }
        }

        /**
         * Returns {@code true} if the next token is an integer, but does not consume it.
         */
        public boolean lookingAtInteger() {
            if (currentToken.length() == 0) {
                return false;
            }

            char ch = currentToken.charAt(0);
            return (('0' <= ch) && (ch <= '9')) || (ch == '-') || (ch == '+');
        }

        /**
         * Returns {@code true} if the next token is a boolean (true/false), but does not consume it.
         */
        public boolean lookingAtBoolean() {
            if (currentToken.length() == 0) {
                return false;
            }

            return "true".equals(currentToken) || "false".equals(currentToken);
        }

        /**
         * Return current token.
         * @return currentToken to which the Tokenizer is pointing
         */
        public String currentToken() {
            return currentToken;
        }

        /**
         * If the next token is an identifier, consume it and return its value. Otherwise, throw a
         * {@link ParseException}.
         */
        public String consumeIdentifier() throws ParseException {
            for (int i = 0; i < currentToken.length(); i++) {
                char ch = currentToken.charAt(i);
                if ((('a' <= ch) && (ch <= 'z'))
                        || (('A' <= ch) && (ch <= 'Z'))
                        || (('0' <= ch) && (ch <= '9'))
                        || (ch == '_')
                        || (ch == '.')
                        || (ch == '"')) {
                    // OK
                } else {
                    throw parseException("Expected identifier. -" + ch);
                }
            }

            String result = currentToken;
            // Need to clean-up result to remove quotes of any kind
            result = result.replaceAll("\"|'", "");
            nextToken();
            return result;
        }

        /**
         * If the next token is a 32-bit signed integer, consume it and return its value. Otherwise,
         * throw a {@link ParseException}.
         */
        public int consumeInt32() throws ParseException {
            try {
                int result = parseInt32(currentToken);
                nextToken();
                return result;
            } catch (NumberFormatException e) {
                throw integerParseException(e);
            }
        }

        /**
         * If the next token is a 32-bit unsigned integer, consume it and return its value.
         * Otherwise, throw a {@link ParseException}.
         */
        public int consumeUInt32() throws ParseException {
            try {
                int result = parseUInt32(currentToken);
                nextToken();
                return result;
            } catch (NumberFormatException e) {
                throw integerParseException(e);
            }
        }

        /**
         * If the next token is a 64-bit signed integer, consume it and return its value. Otherwise,
         * throw a {@link ParseException}.
         */
        public long consumeInt64() throws ParseException {
            try {
                long result = parseInt64(currentToken);
                nextToken();
                return result;
            } catch (NumberFormatException e) {
                throw integerParseException(e);
            }
        }

        /**
         * If the next token is a 64-bit unsigned integer, consume it and return its value.
         * Otherwise, throw a {@link ParseException}.
         */
        public long consumeUInt64() throws ParseException {
            try {
                long result = parseUInt64(currentToken);
                nextToken();
                return result;
            } catch (NumberFormatException e) {
                throw integerParseException(e);
            }
        }

        /**
         * If the next token is a double, consume it and return its value. Otherwise, throw a
         * {@link ParseException}.
         */
        public double consumeDouble() throws ParseException {
            // We need to parse infinity and nan separately because
            // Double.parseDouble() does not accept "inf", "infinity", or "nan".
            if (DOUBLE_INFINITY.matcher(currentToken).matches()) {
                boolean negative = currentToken.startsWith("-");
                nextToken();
                return negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            }
            if (currentToken.equalsIgnoreCase("nan")) {
                nextToken();
                return Double.NaN;
            }
            try {
                double result = Double.parseDouble(currentToken);
                nextToken();
                return result;
            } catch (NumberFormatException e) {
                throw floatParseException(e);
            }
        }

        /**
         * If the next token is a float, consume it and return its value. Otherwise, throw a
         * {@link ParseException}.
         */
        public float consumeFloat() throws ParseException {
            // We need to parse infinity and nan separately because
            // Float.parseFloat() does not accept "inf", "infinity", or "nan".
            if (FLOAT_INFINITY.matcher(currentToken).matches()) {
                boolean negative = currentToken.startsWith("-");
                nextToken();
                return negative ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
            }
            if (FLOAT_NAN.matcher(currentToken).matches()) {
                nextToken();
                return Float.NaN;
            }
            try {
                float result = Float.parseFloat(currentToken);
                nextToken();
                return result;
            } catch (NumberFormatException e) {
                throw floatParseException(e);
            }
        }

        /**
         * If the next token is a boolean, consume it and return its value. Otherwise, throw a
         * {@link ParseException}.
         */
        public boolean consumeBoolean() throws ParseException {
            if (currentToken.equals("true")) {
                nextToken();
                return true;
            } else if (currentToken.equals("false")) {
                nextToken();
                return false;
            } else {
                throw parseException("Expected \"true\" or \"false\".");
            }
        }

        /**
         * If the next token is a string, consume it and return its (unescaped) value. Otherwise,
         * throw a {@link ParseException}.
         */
        public String consumeString() throws ParseException {
            char quote = currentToken.length() > 0 ? currentToken.charAt(0) : '\0';
            if ((quote != '\"') && (quote != '\'')) {
                throw parseException("Expected string.");
            }

            if ((currentToken.length() < 2) || (currentToken.charAt(currentToken.length() - 1) != quote)) {
                throw parseException("String missing ending quote.");
            }

            try {
                String escaped = currentToken.substring(1, currentToken.length() - 1);
                String result = unescapeText(escaped);
                nextToken();
                return result;
            } catch (InvalidEscapeSequence e) {
                throw parseException(e.getMessage());
            }
        }

        /**
         * If the next token is a string, consume it, unescape it as a
         * {@link com.googlecode.protobuf.format.ByteString}, and return it. Otherwise, throw a
         * {@link ParseException}.
         */
        public ByteString consumeByteString() throws ParseException {
            char quote = currentToken.length() > 0 ? currentToken.charAt(0) : '\0';
            if ((quote != '\"') && (quote != '\'')) {
                throw parseException("Expected string.");
            }

            if ((currentToken.length() < 2) || (currentToken.charAt(currentToken.length() - 1) != quote)) {
                throw parseException("String missing ending quote.");
            }

            try {
                String escaped = currentToken.substring(1, currentToken.length() - 1);
                ByteString result = unescapeBytes(escaped);
                nextToken();
                return result;
            } catch (InvalidEscapeSequence e) {
                throw parseException(e.getMessage());
            }
        }

        /**
         * Returns a {@link ParseException} with the current line and column numbers in the
         * description, suitable for throwing.
         */
        public ParseException parseException(String description) {
            // Note: People generally prefer one-based line and column numbers.
            return new ParseException((line + 1) + ":" + (column + 1) + ": " + description);
        }

        /**
         * Returns a {@link ParseException} with the line and column numbers of the previous token
         * in the description, suitable for throwing.
         */
        public ParseException parseExceptionPreviousToken(String description) {
            // Note: People generally prefer one-based line and column numbers.
            return new ParseException((previousLine + 1) + ":" + (previousColumn + 1) + ": " + description);
        }

        /**
         * Constructs an appropriate {@link ParseException} for the given {@code
         * NumberFormatException} when trying to parse an integer.
         */
        private ParseException integerParseException(NumberFormatException exception) {
            return parseException("Couldn't parse integer: " + exception.getMessage());
        }

        /**
         * Constructs an appropriate {@link ParseException} for the given {@code
         * NumberFormatException} when trying to parse a float or double.
         */
        private ParseException floatParseException(NumberFormatException exception) {
            return parseException("Couldn't parse number: " + exception.getMessage());
        }
    }

    /**
     * Thrown when parsing an invalid text format message.
     */
    public static class ParseException extends IOException {

        private static final long serialVersionUID = 1L;

        public ParseException(String message) {
            super(message);
        }
    }

    /**
     * Parse a text-format message from {@code input} and merge the contents into {@code builder}.
     */
    public static void merge(Readable input, Message.Builder builder) throws IOException {
        merge(input, ExtensionRegistry.getEmptyRegistry(), builder);
    }

    /**
     * Parse a text-format message from {@code input} and merge the contents into {@code builder}.
     */
    public static void merge(CharSequence input, Message.Builder builder) throws ParseException {
        merge(input, ExtensionRegistry.getEmptyRegistry(), builder);
    }

    /**
     * Parse a text-format message from {@code input} and merge the contents into {@code builder}.
     * Extensions will be recognized if they are registered in {@code extensionRegistry}.
     */
    public static void merge(CharSequence input, ExtensionRegistry extensionRegistry, Message.Builder builder)
            throws ParseException {
        Tokenizer tokenizer = new Tokenizer(input);

        // Based on the state machine @ http://json.org/

        tokenizer.consume("{"); // Needs to happen when the object starts.
        while (!tokenizer.tryConsume("}")) { // Continue till the object is done
            mergeField(tokenizer, extensionRegistry, builder);
        }
        // Test to make sure the tokenizer has reached the end of the stream.
        if (!tokenizer.atEnd()) {
            throw tokenizer.parseException("Expecting the end of the stream, but there seems to be more data! "
                    + "Check the input for a valid JSON format.");
        }
    }

    /**
     * Parse a text-format message from {@code input} and merge the contents into {@code builder}.
     * Extensions will be recognized if they are registered in {@code extensionRegistry}.
     */
    public static void merge(Readable input, ExtensionRegistry extensionRegistry, Message.Builder builder)
            throws IOException {
        // Read the entire input to a String then parse that.

        // If StreamTokenizer were not quite so crippled, or if there were a kind
        // of Reader that could read in chunks that match some particular regex,
        // or if we wanted to write a custom Reader to tokenize our stream, then
        // we would not have to read to one big String. Alas, none of these is
        // the case. Oh well.

        merge(toStringBuilder(input), extensionRegistry, builder);
    }

    private static final int BUFFER_SIZE = 4096;

    // TODO(chrisn): See if working around java.io.Reader#read(CharBuffer)
    // overhead is worthwhile
    private static StringBuilder toStringBuilder(Readable input) throws IOException {
        StringBuilder text = new StringBuilder();
        CharBuffer buffer = CharBuffer.allocate(BUFFER_SIZE);
        while (true) {
            int num = input.read(buffer);
            if (num == -1) {
                break;
            }
            buffer.flip();
            text.append(buffer, 0, num);
        }
        return text;
    }

    private static final Pattern DIGITS = Pattern.compile("[0-9]", Pattern.CASE_INSENSITIVE);

    /**
     * Parse a single field from {@code tokenizer} and merge it into {@code builder}. If a ',' is
     * detected after the field ends, the next field will be parsed automatically
     */
    private static void mergeField(Tokenizer tokenizer, ExtensionRegistry extensionRegistry, Message.Builder builder)
            throws ParseException {
        FieldDescriptor field;
        Descriptor type = builder.getDescriptorForType();
        ExtensionRegistry.ExtensionInfo extension = null;
        boolean unknown = false;

        String name = tokenizer.consumeIdentifier();
        field = type.findFieldByName(name);

        // Group names are expected to be capitalized as they appear in the
        // .proto file, which actually matches their type names, not their field
        // names.
        if (field == null) {
            // Explicitly specify US locale so that this code does not break when
            // executing in Turkey.
            String lowerName = name.toLowerCase(Locale.US);
            field = type.findFieldByName(lowerName);
            // If the case-insensitive match worked but the field is NOT a group,
            if ((field != null) && (field.getType() != FieldDescriptor.Type.GROUP)) {
                field = null;
            }
        }
        // Again, special-case group names as described above.
        if ((field != null)
                && (field.getType() == FieldDescriptor.Type.GROUP)
                && !field.getMessageType().getName().equals(name)) {
            field = null;
        }

        // Last try to lookup by field-index if 'name' is numeric,
        // which indicates a possible unknown field
        if (field == null && DIGITS.matcher(name).matches()) {
            field = type.findFieldByNumber(Integer.parseInt(name));
            unknown = true;
        }

        // Finally, look for extensions
        extension = extensionRegistry.findExtensionByName(name);
        if (extension != null) {
            if (extension.descriptor.getContainingType() != type) {
                throw tokenizer.parseExceptionPreviousToken(
                        "Extension \"" + name + "\" does not extend message type \"" + type.getFullName() + "\".");
            }
            field = extension.descriptor;
        }

        // Disabled throwing exception if field not found, since it could be a different version.
        if (field == null) {
            handleMissingField(tokenizer, extensionRegistry, builder);
            // throw tokenizer.parseExceptionPreviousToken("Message type \"" + type.getFullName()
            //                                            + "\" has no field named \"" + name
            //                                            + "\".");
        }

        if (field != null) {
            tokenizer.consume(":");
            boolean array = tokenizer.tryConsume("[");

            if (array) {
                while (!tokenizer.tryConsume("]")) {
                    handleValue(tokenizer, extensionRegistry, builder, field, extension, unknown);
                    tokenizer.tryConsume(",");
                }
            } else {
                handleValue(tokenizer, extensionRegistry, builder, field, extension, unknown);
            }
        }

        if (tokenizer.tryConsume(",")) {
            // Continue with the next field
            mergeField(tokenizer, extensionRegistry, builder);
        }
    }

    private static void handleMissingField(
            Tokenizer tokenizer, ExtensionRegistry extensionRegistry, Message.Builder builder) throws ParseException {
        tokenizer.tryConsume(":");
        if ("{".equals(tokenizer.currentToken())) {
            // Message structure
            tokenizer.consume("{");
            do {
                tokenizer.consumeIdentifier();
                handleMissingField(tokenizer, extensionRegistry, builder);
            } while (tokenizer.tryConsume(","));
            tokenizer.consume("}");
        } else if ("[".equals(tokenizer.currentToken())) {
            // Collection
            tokenizer.consume("[");
            do {
                handleMissingField(tokenizer, extensionRegistry, builder);
            } while (tokenizer.tryConsume(","));
            tokenizer.consume("]");
        } else { // if (!",".equals(tokenizer.currentToken)){
            // Primitive value
            if ("null".equals(tokenizer.currentToken())) {
                tokenizer.consume("null");
            } else if (tokenizer.lookingAtInteger()) {
                tokenizer.consumeInt64();
            } else if (tokenizer.lookingAtBoolean()) {
                tokenizer.consumeBoolean();
            } else {
                tokenizer.consumeString();
            }
        }
    }

    private static void handleValue(
            Tokenizer tokenizer,
            ExtensionRegistry extensionRegistry,
            Message.Builder builder,
            FieldDescriptor field,
            ExtensionRegistry.ExtensionInfo extension,
            boolean unknown)
            throws ParseException {

        Object value = null;
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            value = handleObject(tokenizer, extensionRegistry, builder, field, extension, unknown);
        } else {
            value = handlePrimitive(tokenizer, field);
        }
        if (value != null) {
            if (field.isRepeated()) {
                builder.addRepeatedField(field, value);
            } else {
                builder.setField(field, value);
            }
        }
    }

    private static Object handlePrimitive(Tokenizer tokenizer, FieldDescriptor field) throws ParseException {
        Object value = null;
        if ("null".equals(tokenizer.currentToken())) {
            tokenizer.consume("null");
            return value;
        }
        switch (field.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
                value = tokenizer.consumeInt32();
                break;

            case INT64:
            case SINT64:
            case SFIXED64:
                value = tokenizer.consumeInt64();
                break;

            case UINT32:
            case FIXED32:
                value = tokenizer.consumeUInt32();
                break;

            case UINT64:
            case FIXED64:
                /*
                 * HACK: We sometimes need to insert negative numbers into a fixed64 or a uint64, so we need
                 * to accept them here. Ideally we would define these schema as a sfixed64 instead,
                 * but until then we need duplicate Google's method of storing the two's complement.
                 */
                value = tokenizer.consumeInt64();
                break;

            case FLOAT:
                value = tokenizer.consumeFloat();
                break;

            case DOUBLE:
                value = tokenizer.consumeDouble();
                break;

            case BOOL:
                value = tokenizer.consumeBoolean();
                break;

            case STRING:
                value = tokenizer.consumeString();
                break;

            case BYTES:
                value = tokenizer.consumeByteString();
                break;

            case ENUM: {
                EnumDescriptor enumType = field.getEnumType();

                if (tokenizer.lookingAtInteger()) {
                    int number = tokenizer.consumeInt32();
                    value = enumType.findValueByNumber(number);
                    if (value == null) {
                        throw tokenizer.parseExceptionPreviousToken(
                                "Enum type \"" + enumType.getFullName() + "\" has no value with number " + number + "");
                    }
                } else {
                    String id = tokenizer.consumeIdentifier();
                    value = enumType.findValueByName(id);
                    if (value == null) {
                        throw tokenizer.parseExceptionPreviousToken(
                                "Enum type \"" + enumType.getFullName() + "\" has no value named \"" + id + "\".");
                    }
                }

                break;
            }

            case MESSAGE:
            case GROUP:
            default:
                throw new SafeRuntimeException("Can't get here.");
        }
        return value;
    }

    private static Object handleObject(
            Tokenizer tokenizer,
            ExtensionRegistry extensionRegistry,
            Message.Builder builder,
            FieldDescriptor field,
            ExtensionRegistry.ExtensionInfo extension,
            boolean unknown)
            throws ParseException {

        Message.Builder subBuilder;
        if (extension == null) {
            subBuilder = builder.newBuilderForField(field);
        } else {
            subBuilder = extension.defaultInstance.newBuilderForType();
        }

        if (unknown) {
            ByteString data = tokenizer.consumeByteString();
            try {
                subBuilder.mergeFrom(data);
                return subBuilder.build();
            } catch (InvalidProtocolBufferException e) {
                throw tokenizer.parseException("Failed to build " + field.getFullName() + " from " + data);
            }
        }

        tokenizer.consume("{");
        String endToken = "}";

        while (!tokenizer.tryConsume(endToken)) {
            if (tokenizer.atEnd()) {
                throw tokenizer.parseException("Expected \"" + endToken + "\".");
            }
            mergeField(tokenizer, extensionRegistry, subBuilder);
            if (tokenizer.tryConsume(",")) {
                // there are more fields in the object, so continue
                continue;
            }
        }

        return subBuilder.build();
    }

    // =================================================================
    // Utility functions
    //
    // Some of these methods are package-private because Descriptors.java uses
    // them.

    /**
     * Escapes bytes in the format used in protocol buffer text format, which is the same as the
     * format used for C string literals. All bytes that are not printable 7-bit ASCII characters
     * are escaped, as well as backslash, single-quote, and double-quote characters. Characters for
     * which no defined short-hand escape sequence is defined will be escaped using 3-digit octal
     * sequences.
     */
    static String escapeBytes(ByteString input) {
        // modified from the original version of this class to base64 encode instead of utf-8 encode
        return BaseEncoding.base64().encode(input.toByteArray());
    }

    static String unicodeEscaped(char ch) {
        if (ch < 0x10) {
            return "\\u000" + Integer.toHexString(ch);
        } else if (ch < 0x100) {
            return "\\u00" + Integer.toHexString(ch);
        } else if (ch < 0x1000) {
            return "\\u0" + Integer.toHexString(ch);
        }
        return "\\u" + Integer.toHexString(ch);
    }

    /**
     * Un-escape a byte sequence as escaped usin
     * {@link #escapeBytes(com.googlecode.protobuf.format.ByteString)}. Two-digit hex escapes (starting with
     * "\x") are also recognized.
     */
    static ByteString unescapeBytes(CharSequence input) throws InvalidEscapeSequence {
        return ByteString.copyFrom(BaseEncoding.base64().decode(input));
    }

    /**
     * Thrown by {@link ForkedJsonFormat#unescapeBytes} and {@link ForkedJsonFormat#unescapeText} when an
     * invalid escape sequence is seen.
     */
    static class InvalidEscapeSequence extends IOException {

        private static final long serialVersionUID = 1L;

        InvalidEscapeSequence(String description) {
            super(description);
        }
    }

    /**
     * Implements JSON string escaping as specified <a href="http://www.ietf.org/rfc/rfc4627.txt">here</a>.
     * <ul>
     * <li>The following characters are escaped by prefixing them with a '\' : \b,\f,\n,\r,\t,\,"</li>
     * <li>Other control characters in the range 0x0000-0x001F are escaped using the \\uXXXX notation</li>
     * <li>UTF-16 surrogate pairs are encoded using the \\uXXXX\\uXXXX notation</li>
     * <li>any other character is printed as-is</li>
     * </ul>
     */
    private static String escapeText(String input) {
        StringBuilder builder = new StringBuilder(input.length());
        CharacterIterator iter = new StringCharacterIterator(input);
        for (char c = iter.first(); c != CharacterIterator.DONE; c = iter.next()) {
            switch (c) {
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '"':
                    builder.append("\\\"");
                    break;
                default:
                    // Check for other control characters
                    if (c >= 0x0000 && c <= 0x001F) {
                        appendEscapedUnicode(builder, c);
                    } else if (Character.isHighSurrogate(c)) {
                        // Encode the surrogate pair using 2 six-character sequence (\\uXXXX\\uXXXX)
                        appendEscapedUnicode(builder, c);
                        char next = iter.next();
                        if (next == CharacterIterator.DONE) {
                            throw new SafeIllegalArgumentException("invalid unicode string: unexpected high surrogate "
                                    + "pair value without corresponding low value.");
                        }
                        appendEscapedUnicode(builder, next);
                    } else {
                        // Anything else can be printed as-is
                        builder.append(c);
                    }
                    break;
            }
        }
        return builder.toString();
    }

    static void appendEscapedUnicode(StringBuilder builder, char ch) {
        String prefix = "\\u";
        if (ch < 0x10) {
            prefix = "\\u000";
        } else if (ch < 0x100) {
            prefix = "\\u00";
        } else if (ch < 0x1000) {
            prefix = "\\u0";
        }
        builder.append(prefix).append(Integer.toHexString(ch));
    }

    /**
     * Un-escape a text string as escaped using {@link #escapeText(String)}.
     */
    @SuppressWarnings("checkstyle:modifiedcontrolvariable")
    static String unescapeText(String input) throws InvalidEscapeSequence {
        StringBuilder builder = new StringBuilder();
        char[] array = input.toCharArray();
        for (int i = 0; i < array.length; i++) {
            char ch = array[i];
            if (ch == '\\') {
                if (i + 1 < array.length) {
                    ++i;
                    ch = array[i];
                    switch (ch) {
                        case 'b':
                            builder.append('\b');
                            break;
                        case 'f':
                            builder.append('\f');
                            break;
                        case 'n':
                            builder.append('\n');
                            break;
                        case 'r':
                            builder.append('\r');
                            break;
                        case 't':
                            builder.append('\t');
                            break;
                        case '\\':
                            builder.append('\\');
                            break;
                        case '"':
                            builder.append('\"');
                            break;
                        case '\'':
                            builder.append('\'');
                            break;
                        case 'u':
                            // read the next 4 chars
                            if (i + 4 < array.length) {
                                ++i;
                                int code = Integer.parseInt(new String(array, i, 4), 16);
                                // this cast is safe because we know how many chars we read
                                builder.append((char) code);
                                i += 3;
                            } else {
                                throw new InvalidEscapeSequence("Invalid escape sequence: '\\u' at end of string.");
                            }
                            break;
                        default:
                            throw new InvalidEscapeSequence("Invalid escape sequence: '\\" + ch + "'");
                    }
                } else {
                    throw new InvalidEscapeSequence("Invalid escape sequence: '\\' at end of string.");
                }
            } else {
                builder.append(ch);
            }
        }

        return builder.toString();
    }

    /**
     * Parse a 32-bit signed integer from the text. Unlike the Java standard {@code
     * Integer.parseInt()}, this function recognizes the prefixes "0x" and "0" to signify
     * hexidecimal and octal numbers, respectively.
     */
    static int parseInt32(String text) throws NumberFormatException {
        return (int) parseInteger(text, true, false);
    }

    /**
     * Parse a 32-bit unsigned integer from the text. Unlike the Java standard {@code
     * Integer.parseInt()}, this function recognizes the prefixes "0x" and "0" to signify
     * hexidecimal and octal numbers, respectively. The result is coerced to a (signed) {@code int}
     * when returned since Java has no unsigned integer type.
     */
    static int parseUInt32(String text) throws NumberFormatException {
        return (int) parseInteger(text, false, false);
    }

    /**
     * Parse a 64-bit signed integer from the text. Unlike the Java standard {@code
     * Integer.parseInt()}, this function recognizes the prefixes "0x" and "0" to signify
     * hexidecimal and octal numbers, respectively.
     */
    static long parseInt64(String text) throws NumberFormatException {
        return parseInteger(text, true, true);
    }

    /**
     * Parse a 64-bit unsigned integer from the text. Unlike the Java standard {@code
     * Integer.parseInt()}, this function recognizes the prefixes "0x" and "0" to signify
     * hexidecimal and octal numbers, respectively. The result is coerced to a (signed) {@code long}
     * when returned since Java has no unsigned long type.
     */
    static long parseUInt64(String text) throws NumberFormatException {
        return parseInteger(text, false, true);
    }

    private static long parseInteger(String text, boolean isSigned, boolean isLong) throws NumberFormatException {
        int pos = 0;

        boolean negative = false;
        if (text.startsWith("-", pos)) {
            if (!isSigned) {
                throw new NumberFormatException("Number must be positive: " + text);
            }
            ++pos;
            negative = true;
        }

        int radix = 10;
        if (text.startsWith("0x", pos)) {
            pos += 2;
            radix = 16;
        } else if (text.startsWith("0", pos)) {
            radix = 8;
        }

        String numberText = text.substring(pos);

        long result = 0;
        if (numberText.length() < 16) {
            // Can safely assume no overflow.
            result = Long.parseLong(numberText, radix);
            if (negative) {
                result = -result;
            }

            // Check bounds.
            // No need to check for 64-bit numbers since they'd have to be 16 chars
            // or longer to overflow.
            if (!isLong) {
                if (isSigned) {
                    if ((result > Integer.MAX_VALUE) || (result < Integer.MIN_VALUE)) {
                        throw new NumberFormatException("Number out of range for 32-bit signed integer: " + text);
                    }
                } else {
                    if ((result >= (1L << 32)) || (result < 0)) {
                        throw new NumberFormatException("Number out of range for 32-bit unsigned integer: " + text);
                    }
                }
            }
        } else {
            BigInteger bigValue = new BigInteger(numberText, radix);
            if (negative) {
                bigValue = bigValue.negate();
            }

            // Check bounds.
            if (!isLong) {
                if (isSigned) {
                    if (bigValue.bitLength() > 31) {
                        throw new NumberFormatException("Number out of range for 32-bit signed integer: " + text);
                    }
                } else {
                    if (bigValue.bitLength() > 32) {
                        throw new NumberFormatException("Number out of range for 32-bit unsigned integer: " + text);
                    }
                }
            } else {
                if (isSigned) {
                    if (bigValue.bitLength() > 63) {
                        throw new NumberFormatException("Number out of range for 64-bit signed integer: " + text);
                    }
                } else {
                    if (bigValue.bitLength() > 64) {
                        throw new NumberFormatException("Number out of range for 64-bit unsigned integer: " + text);
                    }
                }
            }

            result = bigValue.longValue();
        }

        return result;
    }
}
