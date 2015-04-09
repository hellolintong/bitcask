package BitCask;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.zip.CRC32;

//日记文件处理类
class BitCask {
	public BitCask(String dir, long maxFileLen, long fileNumber) {
		this.maxFileLen = maxFileLen;
		this.fileNumber = fileNumber;
		this.fileMap = new HashMap<Long, RandomAccessFile>();
		this.dir = dir;
		this.indexFileHandler = new IndexFileHandler(dir);
		this.itemHashTable = new Hash();

		// 获取在目录下的所有日志文件
		File directory = new File(dir);
		File[] fileList = directory.listFiles();
		for (File file : fileList) {
			if (file.isFile()) {
				addFile(file);
			}
		}

		// 补齐其他文件(文件范围为1到fileNumber)
		for (long i = 1; i <= this.fileNumber; ++i) {
			File file = new File(dir + getPathSep() + i + ".log");
			if (!file.exists()) {
				addFile(file);
			}
		}

		this.activeFileId = getNewFileId();
		// 所有文件都被使用
		if (this.activeFileId == -1) {
			merge();
			this.activeFileId = getNewFileId();
		}
	}

	// bitcast对外开放的接口
	// 将key与value加入到bitcask中
	public boolean add(String key, String value) {
		if (key == null || value == null) {
			return false;
		}
		Item item = append(key, value);
		itemHashTable.addItem(key, item);
		return true;
	}

	// 获取key对应的value值
	public String get(String key) {
		if (key == null) {
			return null;
		}
		Item item = itemHashTable.getItem(key);
		String str = getValue(item);
		return str;
	}

	// 将key和value信息添加到当前活跃文件（activeFile）中
	private Item append(String key, String value) {
		try {
			RandomAccessFile activeRandomFile = fileMap.get(activeFileId);
			// 如果当前文件超过长度限制，则要更换当前活跃文件
			if (activeRandomFile.length() > maxFileLen) {
				createActiveFile();
			}
			long tsTamp = System.currentTimeMillis();
			long fileId = activeFileId;
			long valueSize = value.length();
			long keySize = key.length();
			long crc = crc32(tsTamp, keySize, valueSize, key, value);
			long startPos = 0;

			activeRandomFile = fileMap.get(fileId);
			startPos = activeRandomFile.length();
			activeRandomFile.seek(startPos);
			activeRandomFile.writeLong(crc);
			activeRandomFile.writeLong(tsTamp);
			activeRandomFile.writeLong(keySize);
			activeRandomFile.writeLong(valueSize);
			activeRandomFile.writeChars(key);
			activeRandomFile.writeChars(value);
			// 将当前内容添加到内存索引中
			IndexItem indexItem = new IndexItem(tsTamp, startPos, fileId,
			        key);
			indexFileHandler.addIndex(indexItem);
			return new Item(fileId, startPos, tsTamp);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 将记录放入内存中便于后续的查找
		
		return null;
	}

	// 根据item的信息获取对应的value值
	private String getValue(Item item) {
		// 根据文件ID来获取随机读取文件对象
		long fileId = item.getFileId();
		RandomAccessFile readFile = fileMap.get(fileId);
		if (readFile == null) {
			return null;
		}
		long startPos = item.getStartPos();
		try {
			// 读取记录信息
			readFile.seek(startPos);

			long crc = readFile.readLong();
			long tsTamp = readFile.readLong();
			long keySize = readFile.readLong();
			long valueSize = readFile.readLong();
			ArrayList<Character> charArray = new ArrayList<Character>();
			for (int i = 0; i < keySize; ++i) {
				charArray.add(readFile.readChar());
			}
			String key = convertToString(charArray);
			charArray.clear();
			for (int i = 0; i < valueSize; ++i) {
				charArray.add(readFile.readChar());
			}
			String value = convertToString(charArray);
			long calCrc = crc32(tsTamp, keySize, valueSize, key, value);

			// 检查crc和时间戳来判断是否为需要的值
			if (calCrc == crc && tsTamp == item.getTsTamp()) {
				return value;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Item appendToMergeFile(IndexItem indexItem) {
		// 获取旧的item对应的value值
		Item item = new Item(indexItem.getFileId(), indexItem.getStartPos(),
		        indexItem.getTsTamp());
		String value = getValue(item);
		
		item = null;
		// 计算新的item信息
		String key = indexItem.getKey();
		long tsTamp = indexItem.getTsTamp();
		long valueSize = value.length();
		long keySize = key.length();
		long crc = crc32(tsTamp, keySize, valueSize, key, value);
		long startPos = 0;
		try {
			RandomAccessFile randomWriteFile = fileMap.get(mergeFileId);
			startPos = randomWriteFile.length();
			randomWriteFile.seek(startPos);
			randomWriteFile.writeLong(crc);
			randomWriteFile.writeLong(tsTamp);
			randomWriteFile.writeLong(keySize);
			randomWriteFile.writeLong(valueSize);
			randomWriteFile.writeChars(key);
			randomWriteFile.writeChars(value);
			// 并将新元素写入到备份hash表中
			item = new Item(mergeFileId, startPos, tsTamp);
			itemHashTable.backItem(key, item);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return item;
	}

	public static String convertToString(ArrayList<Character> charArray) {
		StringBuilder sb = new StringBuilder();
		for (Character ch : charArray) {
			sb.append(ch);
		}
		return sb.toString();
	}

	public static String getPathSep() {
		// Properties PROPERTIES = new Properties(System.getProperties());
		// return PROPERTIES.getProperty("path.separator");
		return "\\";
	}

	// 因为当前文件的长度超过限制，所以创建一个新的文件
	private void createActiveFile() {
		// 将活跃文件在内存中的索引信息全部填入文件中
		indexFileHandler.mergeCurIndex();
		// 获取新的active 文件
		activeFileId = getNewFileId();

		// 如果所有文件都已经写完，则进行合并操作
		if (activeFileId == -1) {
			merge();
			activeFileId = getNewFileId();
		}
	}

	// 将所有旧的文件整合到一个新的文件中，并创建新的索引文件
	private void merge() {

		mergeFlag = true;

		// 创建merge文件并将其添加到fileMap中
		mergeFileId = incMaxFileId();
		createNewFile(mergeFileId);

		// 开始压缩index文件
		indexFileHandler.compress(this);
		// 更新itemHashTabel
		itemHashTable.swap();

		Iterator<Entry<Long, RandomAccessFile>> iter = fileMap.entrySet()
		        .iterator();

		// 清空旧的日志文件
		while (iter.hasNext()) {
			Entry<Long, RandomAccessFile> entry = iter.next();
			try {
				// 如果是merge文件,则跳过
				if (entry.getKey() == mergeFileId) {
					continue;
				}
				// 清空旧的日志文件
				entry.getValue().setLength(0);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		mergeFlag = false;
	}

	// 创建新文件，并将其添加到fileMap中
	private boolean createNewFile(long fileId) {
		File file = new File(dir + getPathSep() + fileId + ".log");
		try {
			if (file.createNewFile()) {
				long k = addFile(file);
				return k > 0 ? true : false;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private long incMaxFileId() {
		return ++fileNumber;
	}

	// 获取当前可用的文件信息
	private long getNewFileId() {
		Iterator<Entry<Long, RandomAccessFile>> iter = fileMap.entrySet()
		        .iterator();
		while (iter.hasNext()) {
			Entry<Long, RandomAccessFile> entry = iter.next();
			try {
				if (entry.getValue().length() == 0) {
					return entry.getKey();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return -1;
	}

	// 判断字符串是否为数字组成
	private boolean isDigital(String str) {
		try {
			long d = Long.parseLong(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

	// 计算记录crc的值
	private long crc32(long tsTamp, long keySize, long valueSize, String key,
	        String value) {
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

	// 添加文件到hash表中，从而便于快速根据ID找到随机读写文件，避免了重复创建（缓存池）
	private long addFile(File file) {
		String filename = file.getName();
		// 先判断是否为log结尾的文件，以及文件名是否为数字
		if (filename != null && filename.length() > 0) {
			int dot = filename.lastIndexOf('.');
			if (dot > -1 && dot < filename.length() - 1) {
				String suffix = filename.substring(dot + 1);
				String prefix = filename.substring(0, dot);
				if (suffix.equals("log") && isDigital(prefix)) {
					long fileId = Long.parseLong(prefix);
					RandomAccessFile randomFile = null;
					try {
						randomFile = new RandomAccessFile(file, "rw");
					} catch (FileNotFoundException e) {
						e.printStackTrace();
						return -1;
					}
					fileMap.put(fileId, randomFile);
					return fileId;
				}
			}
		}
		return -1;
	}

	private boolean mergeFlag;// 在合并文件期间，所有的文件读取操作会被阻止（会返回一个特殊的标志，客户端接收到此标志后，会等待一段时间后再次发起请求）
	private IndexFileHandler indexFileHandler;// index文件处理类
	private Hash itemHashTable;// 元素哈希表（用于记录key在文件中的信息）
	private String dir;// 日志文件目录
	private long activeFileId;// 当前活跃文件id
	private long mergeFileId;// 合并文件id
	private long maxFileLen;// 允许最长的文件长度
	private long fileNumber;// 文件数量(可变)
	private HashMap<Long, RandomAccessFile> fileMap;// 通过文件ID获取随机读取文件对象
}