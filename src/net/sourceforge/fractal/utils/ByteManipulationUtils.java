package net.sourceforge.fractal.utils;

public class ByteManipulationUtils {

	public static String valueOf(byte[] seq) {
		if (seq == null) {
			return null;
		} 
		StringBuffer buff = new StringBuffer();

		for (int i = 0; i < seq.length; i++) {
			buff.append(valueOf(seq[i], true));
		} 
		return buff.toString();
	}

	public static String valueOf(byte num, boolean padding) {
		String hex = Integer.toHexString((int)num);

		if (padding) {
			hex = "00" + hex;
			int len = hex.length();

			hex = hex.substring(len - 2, len);
		} 
		return hex;
	}
	
	
}
