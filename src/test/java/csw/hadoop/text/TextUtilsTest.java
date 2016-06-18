package csw.hadoop.text;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link TextUtils}
 */
public class TextUtilsTest {
	static final String INPUT_STR_ASCII = "abcdef";

	// UTF8 byte lengths: 4+1+4+2+4+3+4, 7 UTF8 chars in total
	static final String INPUT_STR_UTF8 = "𠜎a𠜎Æ𠜎⁂𠜎";

	Text src = new Text();
	Text dst = new Text();

	/**
	 * Happy path tests for
	 * {@link TextUtils#asciiSubstring(Text, Text, int, int)}
	 */
	@Test
	public void testAsciiSubstring() {
		src.set(INPUT_STR_ASCII);
		for (int x = 0; x < INPUT_STR_ASCII.length() - 2; x++) {
			Assert.assertEquals(INPUT_STR_ASCII.substring(x, x + 2),
					TextUtils.asciiSubstring(src, dst, x, x + 2).toString());
		}
	}

	/**
	 * Error test for {@link TextUtils#asciiSubstring(Text, Text, int, int)}
	 */
	@Test(expected = StringIndexOutOfBoundsException.class)
	public void testAsciiSubstringUnderbounds() {
		TextUtils.asciiSubstring(new Text("abcdef"), dst, -1, 0);
	}

	/**
	 * Error test for {@link TextUtils#asciiSubstring(Text, Text, int, int)}
	 */
	@Test(expected = StringIndexOutOfBoundsException.class)
	public void testAsciiSubstringOverbounds() {
		TextUtils.asciiSubstring(new Text("abcdef"), dst, 0, 7);
	}

	/**
	 * Tests for {@link TextUtils#utf8Substring(Text, Text, int, int)}
	 * 
	 * @throws ArrayIndexOutOfBoundsException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testUtf8Substring() throws ArrayIndexOutOfBoundsException, IllegalArgumentException, IOException {
		src.set(INPUT_STR_UTF8);

		// single UTF8 character substrings

		// first UTF8 char (4 bytes, 2 java char)
		Assert.assertEquals(INPUT_STR_UTF8.substring(0, 2), TextUtils.utf8Substring(src, dst, 0, 1).toString());
		// 2nd UTF8 char (1 byte, 1 java char)
		Assert.assertEquals(INPUT_STR_UTF8.substring(2, 3), TextUtils.utf8Substring(src, dst, 1, 2).toString());
		// 4th UTF8 char (2 byte, 1 java char)
		Assert.assertEquals(INPUT_STR_UTF8.substring(5, 6), TextUtils.utf8Substring(src, dst, 3, 4).toString());
		// 6th 3-byte UTF-8
		Assert.assertEquals(INPUT_STR_UTF8.substring(8, 9), TextUtils.utf8Substring(src, dst, 5, 6).toString());
		// last 4-byte UTF-8
		Assert.assertEquals(INPUT_STR_UTF8.substring(9, 11), TextUtils.utf8Substring(src, dst, 6, 7).toString());

		// 2 UTF8 character substrings
		// first two chars (4 + 1 byte, 2 + 1 chars)
		Assert.assertEquals(INPUT_STR_UTF8.substring(0, 3), TextUtils.utf8Substring(src, dst, 0, 2).toString());
		// middle two chars (4 + 3 bytes, 2 + 1 java chars)
		Assert.assertEquals(INPUT_STR_UTF8.substring(5, 8), TextUtils.utf8Substring(src, dst, 3, 5).toString());
		// last two chars (3 + 4 bytes, 1 + 2 java chars)
		Assert.assertEquals(INPUT_STR_UTF8.substring(8, 11), TextUtils.utf8Substring(src, dst, 5, 7).toString());

		// full length substring (effectively a full string copy)
		Assert.assertEquals(INPUT_STR_UTF8.substring(0, 11), TextUtils.utf8Substring(src, dst, 0, 7).toString());
	}

	/**
	 * Error test for {@link TextUtils#utf8Substring(Text, Text, int, int)}
	 */
	@Test(expected = StringIndexOutOfBoundsException.class)
	public void testUtf8SubstringUnderbounds() {
		TextUtils.utf8Substring(new Text(INPUT_STR_UTF8), dst, -1, 0);
	}

	/**
	 * Error test for {@link TextUtils#utf8Substring(Text, Text, int, int)}
	 */
	@Test(expected = StringIndexOutOfBoundsException.class)
	public void testUtf8SubstringOverbounds() {
		TextUtils.utf8Substring(new Text(INPUT_STR_UTF8), dst, 0, 8);
	}

	/**
	 * Tests for {@link TextUtils#asciiTrim(Text)} &
	 * {@link TextUtils#asciiTrim(Text, Text)}
	 */
	@Test
	public void testAsciiTrim() {
		// leading and trailing whitespace
		String strToTrim = "\t AB CD \t\r\n";
		src.set(strToTrim);
		Assert.assertEquals(strToTrim.trim(), TextUtils.asciiTrim(src, dst).toString());
		Assert.assertEquals(strToTrim.trim(), TextUtils.asciiTrim(src).toString());

		// leading whitespace only
		strToTrim = "\t AB CD";
		src.set(strToTrim);
		Assert.assertEquals(strToTrim.trim(), TextUtils.asciiTrim(src, dst).toString());
		Assert.assertEquals(strToTrim.trim(), TextUtils.asciiTrim(src).toString());

		// trailing whitespace only
		strToTrim = "AB CD\r\n \t";
		src.set(strToTrim);
		Assert.assertEquals(strToTrim.trim(), TextUtils.asciiTrim(src, dst).toString());
		Assert.assertEquals(strToTrim.trim(), TextUtils.asciiTrim(src).toString());
	}

	@Test
	public void testAsciiPointCodeAt() {
		src.set(INPUT_STR_ASCII);

		for (int x = 0; x < INPUT_STR_ASCII.length(); x++) {
			Assert.assertEquals(INPUT_STR_ASCII.codePointAt(x), TextUtils.asciiCodePointAt(src, x));
		}
	}

	@Test(expected = StringIndexOutOfBoundsException.class)
	public void testAsciiPointCodeAtUnderbounds() {
		src.set(INPUT_STR_ASCII);
		TextUtils.asciiCodePointAt(src, -1);
	}

	@Test(expected = StringIndexOutOfBoundsException.class)
	public void testAsciiPointCodeAtOverbounds() {
		src.set(INPUT_STR_ASCII);
		TextUtils.asciiCodePointAt(src, INPUT_STR_ASCII.length());
	}

	@Test
	public void testUtf8PointCodeAt() {
		src.set(INPUT_STR_UTF8);

		int strX = 0;
		int textX = 0;
		for (; strX < INPUT_STR_UTF8.length();) {
			Assert.assertEquals(INPUT_STR_UTF8.codePointAt(strX), TextUtils.utf8CodePointAt(src, textX++));
			if (Character.isHighSurrogate(INPUT_STR_UTF8.charAt(strX))) {
				strX += 2;
			} else {
				strX++;
			}
		}
	}
}
