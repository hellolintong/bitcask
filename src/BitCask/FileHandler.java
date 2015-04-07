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

class FileHandler {
	public FileHandler(String dir, String activeFilename, long maxFileLen) {
		this.maxFileLen = maxFileLen;
		this.fileMap = new HashMap<Long, RandomAccessFile>();
		this.dir = dir;
		this.maxFileId = 0;
		activeFilename = dir + getPathSep() + activeFilename;
		this.activeFileId = addFile(new File(activeFilename));
		this.indexFileHandler = new IndexFileHandler(dir);
		// 获取在目录下的所有文件
		File directory = new File(dir);
		File[] fileList = directory.listFiles();
		for (File file : fileList) {
			if (file.isFile()) {
				addFile(file);
			}
		}
	}

	// 将key和value信息添加到当前活跃文件（activeFile）中
	public Item append(String key, String value) {
		long tsTamp = System.currentTimeMillis();
		long fileId = activeFileId;
		long valueSize = value.length();
		long keySize = key.length();
		long crc = crc32(tsTamp, keySize, valueSize, key, value);

		RandomAccessFile randomWriteFile = null;
		long startPos = 0;
		try {
			randomWriteFile = new RandomAccessFile(new File(dir + getPathSep()
			        + activeFileId + ".log"), "rw");
			startPos = randomWriteFile.length();
			randomWriteFile.seek(startPos);
			randomWriteFile.writeLong(crc);
			randomWriteFile.writeLong(tsTamp);
			randomWriteFile.writeLong(keySize);
			randomWriteFile.writeLong(valueSize);
			randomWriteFile.writeChars(key);
			randomWriteFile.writeChars(value);

			// 将当前内容添加到内存索引中
			IndexItem indexItem = new IndexItem(tsTamp, activeFileId, startPos,
			        key);
			indexFileHandler.addIndex(indexItem);
			// 如果当前文件超过长度限制，则要更换当前活跃文件
			if (randomWriteFile.length() > maxFileLen) {
				createActiveFile();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				randomWriteFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// 将记录放入内存中便于后续的查找
		Item item = new Item(fileId, startPos, tsTamp);
		return item;
	}

	// 根据item的信息获取对应的value值
	public String getValue(Item item) {
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

	public void appendToMergeFile(IndexItem indexItem) {
		Item item = new Item(indexItem.getFileId(), indexItem.getStartPos(),
		        indexItem.getTsTamp());
		String value = getValue(item);
		String key = indexItem.getKey();
		long tsTamp = System.currentTimeMillis();
		long valueSize = value.length();
		long keySize = key.length();
		long crc = crc32(tsTamp, keySize, valueSize, key, value);
		RandomAccessFile randomWriteFile = null;
		long startPos = 0;
		try {
			randomWriteFile = new RandomAccessFile(new File(dir + getPathSep()
			        + mergeFile + ".log"), "rw");
			startPos = randomWriteFile.length();
			randomWriteFile.seek(startPos);
			randomWriteFile.writeLong(crc);
			randomWriteFile.writeLong(tsTamp);
			randomWriteFile.writeLong(keySize);
			randomWriteFile.writeLong(valueSize);
			randomWriteFile.writeChars(key);
			randomWriteFile.writeChars(value);
		} catch (IOException e) {
			e.printStackTrace();
		}
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
		return "//";
	}

	// 因为当前文件的长度超过限制，所以创建一个新的文件
	private void createActiveFile() {
		// 将活跃文件在内存中的索引信息全部填入文件中
		indexFileHandler.mergeCurIndex();

		// 更新当前活跃文件
		activeFileId = getMaxFileId();
		File activeFile = new File(dir + getPathSep() + activeFileId + ".log");
		addFile(activeFile);
	}

	// 将所有旧的文件整合到一个新的文件中，并创建新的索引文件
	private void merge() {
		mergeFlag = true;
		indexFileHandler.compress(this);
		Iterator<Entry<Long, RandomAccessFile>> iter = fileMap.entrySet()
		        .iterator();
		//删除旧的日志文件
		while (iter.hasNext()) {
			Entry<Long, RandomAccessFile> entry = iter.next();
			try {
				//如果是当前活跃文件，则跳过
				if(entry.getKey() == activeFileId){
					continue;
				}
				//关闭随机读写文件
				entry.getValue().close();
				//删除文件
				File f = new File(dir + getPathSep() + entry.getKey() + ".log");
				f.delete();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//更新fileMap数据结构
		fileMap.clear();
		addFile(new File(dir + getPathSep() + activeFileId + ".log"));

		//先将mergeFile重命名为数据文件，然后再加入到fileMap中
		File file = new File(dir + getPathSep() + mergeFile + ".log");
		long mergeFileId = getMaxFileId();
		File mergeFile = new File(dir + getPathSep() + mergeFileId + ".log");
		file.renameTo(mergeFile);//将merge文件重命名为数据文件
		addFile(mergeFile);
		mergeFlag = false;
	}

	private long getMaxFileId(){
		return maxFileId++;
	}

	private boolean isDigital(String str) {
		try {
			long d = Long.parseLong(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

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
						randomFile = new RandomAccessFile(
						        file.getAbsolutePath(), "r");
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
					fileMap.put(fileId, randomFile);
					if (fileId > maxFileId) {
						maxFileId = fileId;
					}
					return fileId;
				}
			}
		}
		return -1;
	}

	private boolean mergeFlag;// 在合并文件期间，所有的文件读取操作会被阻止（会返回一个特殊的标志，客户端接收到此标志后，会等待一段时间后再次发起请求）
	private IndexFileHandler indexFileHandler;// index文件处理类
	private long maxFileId;// 最大的文件ID（用于创建新文件）
	private String dir;// 日志文件目录
	private long activeFileId;// 当前活跃文件
	private long mergeFile;// 合并文件
	private long maxFileLen;// 允许最长的文件长度
	private HashMap<Long, RandomAccessFile> fileMap;// 通过文件ID获取随机读取文件对象
}