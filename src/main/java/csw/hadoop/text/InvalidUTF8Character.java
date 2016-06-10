package csw.hadoop.text;

/**
 * Exception for an invalid UTF8 character byte stream
 */
public class InvalidUTF8Character extends RuntimeException {
	private static final long serialVersionUID = 1L;

	private InvalidUTF8Character(String byteStreamStr) {
		super("Invalid UTF8 character byte stream: " + byteStreamStr);
	}

	/**
	 * Throw exception with constructed message listing the byte stream as a hex
	 * string
	 * 
	 * @param b
	 *            Byte stream for UTF8 string
	 * @param offset
	 *            Offset where the offending UTF8 character byte stream starts
	 * @param length
	 *            Number of suspected bytes for offending UTF8 character
	 */
	public static void throwException(byte[] b, int offset, int length) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Starting at offset 0x");
		strBuilder.append(Integer.toHexString(offset));
		strBuilder.append(" for ");
		strBuilder.append(length);
		strBuilder.append(" bytes [");

		for (int x = 0; x < length; x++) {
			strBuilder.append(' ');
			strBuilder.append(Integer.toHexString(b[offset + 1] & 0xFF));
		}

		strBuilder.append(" ]");

		throw new InvalidUTF8Character(strBuilder.toString());
	}
}
