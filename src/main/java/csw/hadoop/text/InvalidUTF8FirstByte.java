package csw.hadoop.text;

/**
 * Exception for an invalid leading UTF8 byte character
 */
public class InvalidUTF8FirstByte extends RuntimeException {
	private static final long serialVersionUID = 1L;
	private int byteValue;

	public InvalidUTF8FirstByte(int byteValue) {
		super("First UTF8 character byte is invalid: " + Integer.toHexString(byteValue));
		this.byteValue = byteValue;
	}

	public int getByteValue() {
		return byteValue;
	}
}
