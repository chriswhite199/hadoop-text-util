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

	// 1 byte(ascii) + 2 byte + 3 byte + 4 byte UTF8 characts, which is a 5
	// characters in Java due to UTF-16 chars and the final 4 byte UTF8
	// character is actually 2 characters in Java
	static final String INPUT_STR_UTF8 = "aÆ⁂𠜎";

	Text src = new Text();
	Text dst = new Text();

	/**
	 * Happy path tests for
	 * {@link TextUtils#asciiSubstring(Text, Text, int, int)}
	 */
	@Test
	public void testSubstringAscii() {
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
	public void testSubstringAsciiUnderbounds() {
		TextUtils.asciiSubstring(new Text("abcdef"), dst, -1, 0);
	}

	/**
	 * Error test for {@link TextUtils#asciiSubstring(Text, Text, int, int)}
	 */
	@Test(expected = StringIndexOutOfBoundsException.class)
	public void testSubstringAsciiOverbounds() {
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
	public void testSubstringUTF8() throws ArrayIndexOutOfBoundsException, IllegalArgumentException, IOException {
		src.set(INPUT_STR_UTF8);

		// sanity check bytes length of string literal
		Assert.assertEquals(INPUT_STR_UTF8.getBytes(StandardCharsets.UTF_8).length, src.getLength());

		// first ascii byte
		Assert.assertEquals(INPUT_STR_UTF8.substring(0, 1), TextUtils.utf8Substring(src, dst, 0, 1).toString());
		// second 2-byte UTF-8
		Assert.assertEquals(INPUT_STR_UTF8.substring(1, 2), TextUtils.utf8Substring(src, dst, 1, 2).toString());
		// third 3-byte UTF-8
		Assert.assertEquals(INPUT_STR_UTF8.substring(2, 3), TextUtils.utf8Substring(src, dst, 2, 3).toString());
		// forth 4-byte UTF-8
		// Java uses 2 chars for 4 byte UTF characters
		Assert.assertEquals(INPUT_STR_UTF8.substring(3, 5), TextUtils.utf8Substring(src, dst, 3, 4).toString());
	}

	/**
	 * Tests for {@link TextUtils#asciiTrim(Text)} &
	 * {@link TextUtils#asciiTrim(Text, Text)}
	 */
	@Test
	public void testTrimAscii() {
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
}
