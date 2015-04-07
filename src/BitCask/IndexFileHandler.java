package BitCask;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

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

class IndexFileHandler {
	public IndexFileHandler(String dir) {
		this.dir = dir;
		try {
			indexRandomFile = new RandomAccessFile(dir + FileHandler.getPathSep() + "index.log", "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void addIndex(IndexItem item) {
		String key = item.getKey();
		IndexItem oldItem = indexHashMap.get(key);
		if (oldItem == null || oldItem.getTsTamp() < item.getStartPos()) {
			indexHashMap.put(key, item);
		}
	}

	// 当前活跃文件长度超过限制，所以创建新的活跃文件，故将当前活跃文件中的索引信息写入到索引文件中
	public void mergeCurIndex() {
		Collection<IndexItem> values = indexHashMap.values();
		Iterator<IndexItem> iter = values.iterator();
		while (iter.hasNext()) {
				IndexItem item = iter.next();
				writeToFile(indexRandomFile, item);
		}
		indexHashMap.clear();
	}

	//将内容写入索引文件中
	private void writeToFile(RandomAccessFile randomFile, IndexItem item){
		try{
			long fileLen = randomFile.length();
			randomFile.seek(fileLen);
			randomFile.writeLong(item.getTsTamp());
			randomFile.writeLong(item.getFileId());
			randomFile.writeLong(item.getStartPos());
			String key = item.getKey();
			long keyLen = key.length();
			randomFile.writeLong(keyLen);
			randomFile.writeChars(key);
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}

	//压缩索引文件，去除重复的元素
	//调用FileHandler类的方法，获取索引中对应的value值，并将其写入到新的合并文件中
	//注意：这里的IndexItem和Item是不一样的，因为Item要存留内存，并且只负责读操作，所以我们只要知道文件ID和读取的起始位置就可以直接读取value值，因此不需要记录key信息
	//而IndexItem是索引文件中的内容，所以IndexItem必须要含有key信息，才能去除重复的记录。而且IndexItem只有当前活跃文件的索引信息存储在内存中，所以是不影响的
	
	public boolean compress(FileHandler fileHandler){
		HashMap<String, IndexItem> tempIndexHashMap = new HashMap<String, IndexItem>();
		try {
			long fileLen = indexRandomFile.length();
			indexRandomFile.seek(0);
			//先将所有的内容读取出来，并获取到某个key的最新信息（根据时间戳）
			while (indexRandomFile.getFilePointer() < fileLen) {
				long tsTamp = indexRandomFile.readLong();
				long fileId = indexRandomFile.readLong();
				long startPos = indexRandomFile.readLong();
				long keyLen = indexRandomFile.readLong();
				ArrayList<Character> charArray = new ArrayList<Character>();
				for (int i = 0; i < keyLen; ++i) {
					charArray.add(indexRandomFile.readChar());
				}
				String key = FileHandler.convertToString(charArray);
				IndexItem item = tempIndexHashMap.get(key);
				if(item == null || tsTamp > item.getTsTamp()){
					tempIndexHashMap.put(key, new IndexItem(tsTamp, fileId, startPos, key));
				}
			}
			//创建新的索引文件，并写入信息
			RandomAccessFile newIndexRandomFile = new RandomAccessFile(dir + FileHandler.getPathSep() + "index2.log", "rw");
			Collection<IndexItem> values = tempIndexHashMap.values();
			Iterator<IndexItem> iter = values.iterator();
			while(iter.hasNext()){
				IndexItem item = iter.next();
				writeToFile(newIndexRandomFile, item);
				//同时读取内容文件，并读出对应的value值写入到max
				fileHandler.appendToMergeFile(item);
			}
			
			//删除旧的索引文件，并设置新的索引文件
			indexRandomFile.close();
			File oldIndexFile = new File(dir + FileHandler.getPathSep() + "index.log");
			oldIndexFile.delete();
			File newIndexFile = new File(dir + FileHandler.getPathSep() + "index2.log");
			newIndexFile.renameTo(new File(dir + FileHandler.getPathSep() + "index.log"));
			
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	String dir;
	RandomAccessFile indexRandomFile;
	private HashMap<String, IndexItem> indexHashMap;
}
