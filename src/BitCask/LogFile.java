package BitCask;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

class LogFile {
	public LogFile(String fileName, long fileId) {
		this.fileId = fileId;
		this.file = new File(fileName);
		try {
			this.accessHanlder = new RandomAccessFile(this.file, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	void clear() {
		try {
			accessHanlder.setLength(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 写入item的信息, 返回
	Item writeItem(String key, String value) {
		return writeItem(key, value, -1);
	}

	Item writeItem(String key, String value, long tsTamp) {
		// 将key和value信息添加到当前活跃文件（activeFile）中
		try {
			if (tsTamp < 0) {
				tsTamp = System.currentTimeMillis();
			}
			long valueSize = value.length();
			long keySize = key.length();
			long crc = Utility.crc32(tsTamp, keySize, valueSize, key, value);
			long startPos = getCurLen();
			if (startPos < 0) {
				return null;
			}
			accessHanlder.seek(startPos);
			accessHanlder.writeLong(crc);
			accessHanlder.writeLong(tsTamp);
			accessHanlder.writeLong(keySize);
			accessHanlder.writeLong(valueSize);
			accessHanlder.writeChars(key);
			accessHanlder.writeChars(value);
			return new Item(fileId, startPos, tsTamp);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	// 根据item读取文件中对应的value值，如果item的时间戳与crc和文件读取的内容不配对则返回null
	String readValue(Item item) {
		long startPos = item.getStartPos();
		try {
			if (accessHanlder.length() < startPos) {
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		try {
			accessHanlder.seek(startPos);
			long crc = accessHanlder.readLong();
			long tsTamp = accessHanlder.readLong();
			long keySize = accessHanlder.readLong();
			long valueSize = accessHanlder.readLong();
			ArrayList<Character> charArray = new ArrayList<Character>();
			for (int i = 0; i < keySize; ++i) {
				charArray.add(accessHanlder.readChar());
			}
			String key = Utility.convertToString(charArray);
			charArray.clear();
			for (int i = 0; i < valueSize; ++i) {
				charArray.add(accessHanlder.readChar());
			}
			String value = Utility.convertToString(charArray);
			long calCrc = Utility.crc32(tsTamp, keySize, valueSize, key, value);

			// 检查crc和时间戳来判断是否为需要的值
			if (calCrc == crc && tsTamp == item.getTsTamp()) {
				return value;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	long getCurLen() {
		try {
			return accessHanlder.length();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	// 判断文件名是否满足要求，因为会使用文件名作为文件ID，所以要检查
	static boolean isLogFile(String filename) {
		// 判断是否为log结尾的文件，以及文件名是否为数字
		if (filename != null && filename.length() > 0) {
			int dot = filename.lastIndexOf('.');
			if (dot > -1 && dot < filename.length() - 1) {
				String suffix = filename.substring(dot + 1);
				String prefix = filename.substring(0, dot);
				if (suffix.equals("log") && Utility.isDigital(prefix)) {
					return true;
				}
			}
		}
		return false;
	}

	static long getFileId(String filename) {
		if (!isLogFile(filename)) {
			return -1;
		}
		return Long.parseLong(filename.substring(0, filename.lastIndexOf('.')));
	}

	private Long fileId;
	private File file;
	private RandomAccessFile accessHanlder;
}