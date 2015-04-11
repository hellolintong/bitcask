package BitCask;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

//索引单元
class IndexItem {

	public IndexItem(long tsTamp, long startPos, long fileId, String key) {
		this.tsTamp = tsTamp;
		this.startPos = startPos;
		this.fileId = fileId;
		this.key = key;
	}

	public long getStartPos() {
		return startPos;
	}

	public void setStartPos(long startPos) {
		this.startPos = startPos;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public long getFileId() {
		return fileId;
	}

	public void setFileId(long fileId) {
		this.fileId = fileId;
	}

	public long getTsTamp() {
		return tsTamp;
	}

	public void setTsTamp(long tsTamp) {
		this.tsTamp = tsTamp;
	}

	private long fileId;
	private long tsTamp;
	private long startPos;
	private String key;
}

// 索引处理文件，包括内存当前活跃文件的索引缓存
class IndexFileHandler {
	public IndexFileHandler(String dir) {
		this.indexHashMap = new HashMap<String, IndexItem>();
		this.dir = dir;
		try {
			indexRandomFile = new RandomAccessFile(dir + Utility.getPathSep()
			        + "index.log", "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	// 添加索引到内存中
	public void addIndex(IndexItem item) {
		String key = item.getKey();
		IndexItem oldItem = indexHashMap.get(key);
		if (oldItem == null || oldItem.getTsTamp() < item.getTsTamp()) {
			indexHashMap.put(key, item);
		}
	}

	// 当前活跃文件长度超过限制，所以创建新的活跃文件，故将当前活跃文件中的索引信息写入到索引文件中
	public void mergeCurIndex() {
		Collection<IndexItem> values = indexHashMap.values();
		Iterator<IndexItem> iter = values.iterator();
		while (iter.hasNext()) {
			IndexItem item = iter.next();
			writeToFile(item);
		}
		indexHashMap.clear();
	}

	// 将内容写入索引文件中
	private void writeToFile(IndexItem item) {
		try {
			long fileLen = indexRandomFile.length();
			indexRandomFile.seek(fileLen);
			indexRandomFile.writeLong(item.getTsTamp());
			indexRandomFile.writeLong(item.getFileId());
			indexRandomFile.writeLong(item.getStartPos());
			String key = item.getKey();
			long keyLen = key.length();
			indexRandomFile.writeLong(keyLen);
			indexRandomFile.writeChars(key);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void clearIndexFile() {
		try {
			indexRandomFile.setLength(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 刷新索引（去除重复的)
	private HashMap<String, IndexItem> readAllIndex() {
		HashMap<String, IndexItem> refreshHashMap = new HashMap<String, IndexItem>();
		try {
			long fileLen = indexRandomFile.length();
			indexRandomFile.seek(0);
			while (indexRandomFile.getFilePointer() < fileLen) {
				long tsTamp = indexRandomFile.readLong();
				long fileId = indexRandomFile.readLong();
				long startPos = indexRandomFile.readLong();
				long keyLen = indexRandomFile.readLong();
				ArrayList<Character> charArray = new ArrayList<Character>();
				for (int i = 0; i < keyLen; ++i) {
					charArray.add(indexRandomFile.readChar());
				}
				String key = Utility.convertToString(charArray);
				IndexItem item = refreshHashMap.get(key);
				if (item == null || tsTamp > item.getTsTamp()) {
					refreshHashMap.put(key, new IndexItem(tsTamp, startPos,
					        fileId, key));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return refreshHashMap;
	}

	// 注意：这里的IndexItem和Item是不一样的，因为Item要存留内存，并且只负责读操作，所以我们只要知道文件ID和读取的起始位置就可以直接读取value值，因此不需要记录key信息(尽量减少内存空间占用）
	// 而IndexItem是索引文件中的内容，所以IndexItem必须要含有key信息，才能去除重复的记录。而且IndexItem只有当前活跃文件的索引信息存储在内存中，所以是不影响的
	public void compress(LogFileHandler logFileHandler, Hash hashTable) {
		HashMap<String, IndexItem> tempIndexHashMap = null;
		//先将内存中的索引写入文件
		mergeCurIndex();
		//先获取index的最新索引
		tempIndexHashMap = readAllIndex();
		// 创建新的索引文件，并写入信息
		clearIndexFile();
		Iterator<IndexItem> iter = tempIndexHashMap.values().iterator();
		while (iter.hasNext()) {
			// 注意此处的indexItem和indexItem2的区别
			// indexItem含有的文件id是旧的文件id，之所以要用到他，因为在appendToMergeFile中，要先用该id找出key对应的value然后再写入到新的merg文件中
			// indexItem2中含有的文件id是新的文件id。因为旧的文件会被删除掉
			IndexItem indexItem = iter.next();

			//读取内容文件，并将读出的value值写入到merge file中
			Item item = logFileHandler.appendToMergeFile(indexItem);

			//哈希表也更新信息
			hashTable.backItem(indexItem.getKey(), item);

			// 将更新后的信息写入到index file中
			IndexItem indexItem2 = new IndexItem(item.getTsTamp(),
			        item.getStartPos(), item.getFileId(), indexItem.getKey());
			writeToFile(indexItem2);
		}
	}

	String dir;
	RandomAccessFile indexRandomFile;
	private HashMap<String, IndexItem> indexHashMap;
}
