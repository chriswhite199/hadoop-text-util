package csw.hadoop.text;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

/**
 * Collection of String utilities for Hadoop {@link Text} that is geared to be
 * more performant that using {@link String#toString()} to acheive similar
 * operations.
 * 
 * None of the methods in this class are thread safe (and Hadoop tasks are
 * single threaded)
 */
public class TextUtils {
	/** Singleton array of the last discovered character offsets */
	public static final List<Integer> CHAR_OFFSETS = new ArrayList<Integer>(512);

	/**
	 * LUT for the first byte in a UTF8 character to determine the total number
	 * of bytes in the character
	 */
	static final int[] UTF8_BYTE_LENGTH = new int[256];

	static {
		// Configure UTF8 byte length array
		for (int x = 0; x < 256; x++) {
			if (x <= 0x7F) {
				UTF8_BYTE_LENGTH[x] = 1;
			} else if (x >= 0xC0 && x <= 0xDF) {
				UTF8_BYTE_LENGTH[x] = 2;
			} else if (x >= 0xE0 && x <= 0xEF) {
				UTF8_BYTE_LENGTH[x] = 3;
			} else if (x >= 0xF0 && x <= 0xF7) {
				UTF8_BYTE_LENGTH[x] = 4;
			} else {
				UTF8_BYTE_LENGTH[x] = -1;
			}
		}
	}

	/**
	 * Substring the source text object, and populate the destination text
	 * object.
	 * <p>
	 * Assumes that the underlying byte arrays of the source Text object
	 * represents a UTF-8 string, and the start & end parameters related to the
	 * UTF-8 characters, not the number of raw bytes (and accounting for 4 byte
	 * UTF-8 characters as a single character, not two characters like Java
	 * does)
	 * 
	 * @param src
	 *            Source to substring
	 * @param dst
	 *            Destination to output the substring into to
	 * @param start
	 *            Start UTF-8 character offset
	 * @param end
	 *            End UTF-8 character offset (not including this character)
	 * @return dst Text, for call chaining
	 */
	public static Text utf8Substring(Text src, Text dst, int start, int end) {
		findUTF8CharOffsets(src, CHAR_OFFSETS, end);
		return utf8Substring(src, dst, CHAR_OFFSETS, start, end);
	}

	/**
	 * Substring the source text object, and populate the destination text
	 * object.
	 * <p>
	 * Assumes that the underlying byte arrays of the source Text object
	 * represents a UTF-8 string, and the start & end parameters related to the
	 * UTF-8 characters, not the number of raw bytes (and accounting for 4 byte
	 * UTF-8 characters as a single character, not two characters like Java
	 * does)
	 * 
	 * @param src
	 *            Source to substring
	 * @param dst
	 *            Destination to output the substring into to
	 * @param charOffsets
	 *            Array of UTF-8 character offsets in the underlying src Text
	 *            byte array (as created by
	 *            {@link #findUTF8CharOffsets(Text, List, int)})
	 * @param start
	 *            Start UTF-8 character offset
	 * @param end
	 *            End UTF-8 character offset (not including this character)
	 * @return dst Text, for call chaining
	 */
	public static Text utf8Substring(Text src, Text dst, List<Integer> charOffsets, int start, int end) {
		findUTF8CharOffsets(src, CHAR_OFFSETS, end);

		if (start >= CHAR_OFFSETS.size() || start < 0) {
			throw new StringIndexOutOfBoundsException(start);
		} else if (end > CHAR_OFFSETS.size()) {
			throw new StringIndexOutOfBoundsException(end);
		} else if (end <= start) {
			throw new StringIndexOutOfBoundsException(String.format("end index <= start index: %d <= %d", end, start));
		}

		int firstCharByteOffset = CHAR_OFFSETS.get(start);
		// last entry in CHAR_OFFSETS will be -1 if no more chars in string
		int endCharByteOffset = CHAR_OFFSETS.get(end);
		if (endCharByteOffset == -1) {
			endCharByteOffset = src.getLength();
		}

		dst.clear();
		dst.append(src.getBytes(), firstCharByteOffset, endCharByteOffset - firstCharByteOffset);

		return dst;
	}

	/**
	 * A substring implementation that doesn't bother discovering the character
	 * offsets as the src Text object is assumed to contain a single byte per
	 * character
	 * 
	 * @param src
	 *            Source Text to substring
	 * @param dst
	 *            Destination Text to populate with the substring
	 * @param start
	 *            Substring start offset
	 * @param end
	 *            Substring end offset (not including this character)
	 * @return dst Text, for call chaining
	 */
	public static Text asciiSubstring(Text src, Text dst, int start, int end) {
		if (start < 0) {
			throw new StringIndexOutOfBoundsException(start);
		} else if (end > src.getLength()) {
			throw new StringIndexOutOfBoundsException(end);
		}

		dst.clear();
		dst.append(src.getBytes(), start, end - start);

		return dst;
	}

	/**
	 * Wrapper for {@link #findUTF8CharOffsets(Text, List, int)} with a maxChars
	 * value of {@link Integer#MAX_VALUE}
	 * 
	 * @param src
	 * @param offsets
	 */
	public static void findUTF8CharOffsets(Text src, List<Integer> offsets) {
		findUTF8CharOffsets(src, offsets, Integer.MAX_VALUE);
	}

	/**
	 * Step through the source Text object, locating the character offsets and
	 * populating the target list
	 * 
	 * @param src
	 *            Text object to process
	 * @param offsets
	 *            List to populate with character offsets
	 * @param maxChars
	 *            maximum number of characters to discover (for efficient short
	 *            cutting of logic when only a certain number of characters are
	 *            needed)
	 */
	public static void findUTF8CharOffsets(Text src, List<Integer> offsets, int maxChars) {
		byte[] srcBytes = src.getBytes();
		int charIdx = 0;
		int x;
		offsets.clear();
		for (x = 0; x < src.getLength() && charIdx < maxChars + 1;) {
			offsets.add(x);
			int byteValue = srcBytes[x] & 0xFF;
			int charLength = UTF8_BYTE_LENGTH[byteValue];
			if (charLength == -1) {
				throw new InvalidUTF8FirstByte(byteValue);
			}
			x += charLength;
		}

		if (x == src.getLength()) {
			// add marker to denote end of string
			offsets.add(-1);
		}
	}

	/**
	 * Trim leading / trailing whitespace in the passed text object
	 * 
	 * @param text
	 *            Text object to trim
	 */
	public static Text asciiTrim(Text text) {
		return asciiTrim(text, text);
	}

	/**
	 * Trim leading / trailing whitespace in the passed src object, populating
	 * the destination text object
	 * 
	 * @param src
	 *            Source to trim
	 * @param dst
	 *            Destination to populate with trimmed ascii string
	 */
	public static Text asciiTrim(Text src, Text dst) {
		int len = src.getLength();
		int st = 0;
		byte[] bytes = src.getBytes();

		while ((st < len) && (bytes[st] <= ' ')) {
			st++;
		}
		while ((st < len) && (bytes[len - 1] <= ' ')) {
			len--;
		}

		dst.set(bytes, st, len - st);
		return dst;
	}

	/**
	 * Trim leading / trailing whitespace from the passed text object which
	 * contains UTF8 byte stream
	 * 
	 * @param text
	 *            Object to trim
	 * @return text object, for call chaining
	 */
	public static Text utf8Trim(Text src) {
		return utf8Trim(src, src);
	}

	/**
	 * Get the point code for the given character index, assuming that the
	 * passed Text object only contains ASCII characters
	 * 
	 * @param text
	 *            Source text
	 * @param index
	 *            Character index
	 * @return Point code of ascii character
	 */
	public static int asciiCharAt(Text text, int index) {
		return text.getBytes()[index];
	}

	/**
	 * Get the point code for the UTF8 character
	 * 
	 * @param text
	 *            Text object to query
	 * @param index
	 *            Character index to query
	 * @return point code of UTF8 character at requested index
	 */
	public static int utf8CharAt(Text text, int index) {
		findUTF8CharOffsets(text, CHAR_OFFSETS);
		return utf8CharAt(text, CHAR_OFFSETS, index);
	}

	/**
	 * Get the point code for the UTF8 character
	 * 
	 * @param text
	 *            Text object to query
	 * @param charOffsets
	 *            Pre-populated UTF8 character offsets
	 * @param index
	 *            Character index to query
	 * @return point code of UTF8 character at requested index
	 */
	public static int utf8CharAt(Text text, List<Integer> charOffsets, int index) {
		if (index >= charOffsets.size()) {
			throw new StringIndexOutOfBoundsException(index);
		}

		byte[] bytes = text.getBytes();
		int firstCharByteOffset = charOffsets.get(index);
		int endCharByteOffset = charOffsets.get(index + 1);
		if (endCharByteOffset == -1) {
			endCharByteOffset = text.getLength();
		}

		int pointCode = 0;
		int charLength = endCharByteOffset - firstCharByteOffset;
		switch (charLength) {
		case 1:
			pointCode = bytes[firstCharByteOffset] & 0x7F;
			break;
		case 2:
			pointCode = (bytes[firstCharByteOffset] & 0x1F) << 6;
			pointCode |= (bytes[firstCharByteOffset + 1] & 0x3F);
			break;
		case 3:
			pointCode = (bytes[firstCharByteOffset] & 0x0F) << 12;
			pointCode |= (bytes[firstCharByteOffset + 1] & 0x3F) << 6;
			pointCode |= (bytes[firstCharByteOffset + 2] & 0x3F);
			break;
		case 4:
			pointCode = (bytes[firstCharByteOffset] & 0x0F) << 18;
			pointCode |= (bytes[firstCharByteOffset + 1] & 0x3F) << 12;
			pointCode |= (bytes[firstCharByteOffset + 2] & 0x3F) << 6;
			pointCode |= (bytes[firstCharByteOffset + 3] & 0x3F);
			break;
		default:
			throw new RuntimeException("Unexpected UTF8 charatcer byte length: " + charLength);
		}

		return pointCode;
	}

	/**
	 * Trim leading / trailing whitespace from the passed src object which
	 * contains UTF8 byte stream
	 * 
	 * @param src
	 *            Source to trim
	 * @param dst
	 *            Destination to populate with trimmed UTF8 string
	 * @return Destination text object, for call chaining
	 */
	public static Text utf8Trim(Text src, Text dst) {
		findUTF8CharOffsets(src, CHAR_OFFSETS);

		int len = CHAR_OFFSETS.size();
		int st = 0;
		byte[] bytes = src.getBytes();

		while ((st < len) && (bytes[st] <= ' ')) {
			st++;
		}
		while ((st < len) && (bytes[len - 1] <= ' ')) {
			len--;
		}

		dst.set(bytes, st, len - st);

		return dst;
	}
}
