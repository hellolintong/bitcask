package BitCask;

import java.util.ArrayList;
import java.util.zip.CRC32;

public class Utility {
	// 判断字符串是否为数字组成
	static boolean isDigital(String str) {
		try {
			long d = Long.parseLong(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

	// 计算记录crc的值
	static long crc32(long tsTamp, long keySize, long valueSize,

	String key, String value) {
		StringBuilder sb = new StringBuilder();
		sb.append(tsTamp);
		sb.append(keySize);
		sb.append(valueSize);
		sb.append(key);
		sb.append(value);
		String str = sb.toString();
		CRC32 crc = new CRC32();
		crc.update(str.getBytes());
		return (long) (crc.getValue());
	}

	static String convertToString(ArrayList<Character> charArray) {
		StringBuilder sb = new StringBuilder();
		for (Character ch : charArray) {
			sb.append(ch);
		}
		return sb.toString();
	}
	
	static String getPathSep() {
		// Properties PROPERTIES = new Properties(System.getProperties());
		// return PROPERTIES.getProperty("path.separator");
		return "\\";
	}
	
	static IndexItem itemToIndexItem(Item item, String key){
		return new IndexItem(item.getTsTamp(), item.getStartPos(), item.getFileId(), key);
	}
	
//	static Item indexItemToItem(IndexItem indexItem){
//		
//	}
}