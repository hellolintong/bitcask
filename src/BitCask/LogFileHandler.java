package BitCask;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

class LogFileHandler {

	public LogFileHandler(String dir, long fileMaxLen, long fileNumber) {
		this.dir = dir;
		this.fileMaxLen = fileMaxLen;
		this.fileNumber = fileNumber;
		this.logFileTable = new HashMap<Long, LogFile>();

		// 创建新文件
		for (long i = 1; i <= fileNumber; ++i) {
			String filename = dir + Utility.getPathSep() + i + ".log";
			logFileTable.put(i, new LogFile(filename, i));
		}
	}

	public boolean initActiveFile(){
		return generateActiveFileId();
	}
	
	public Item append(String key, String value){
		LogFile logFile = logFileTable.get(activeFileId);
		if(logFile.getCurLen() > fileMaxLen){
			if(generateActiveFileId() == false){
				return null;
			}
			else{
				logFile = logFileTable.get(activeFileId);
			}
		}
		return logFile.writeItem(key, value);
	}

	public String read(Item item){
		LogFile logFile = logFileTable.get(item.getFileId());
		return logFile.readValue(item);
	}
	
	public void clearLogFile() {
		Iterator<Entry<Long, LogFile>> iter = logFileTable.entrySet()
		        .iterator();
		// 清空旧的日志文件
		while (iter.hasNext()) {
			Entry<Long, LogFile> entry = iter.next();
			// 如果是merge文件,则跳过
			if (entry.getKey() == mergeFileId) {
				continue;
			}
			// 清空旧的日志文件
			entry.getValue().clear();
		}
	}

	// 生成activeFile文件
	private boolean generateActiveFileId() {
		// 设置当前活跃文件ID
		activeFileId = getNewFileId();
		// 判断active文件是否生成成功，如果失败，则外部要进行merge操作
		return activeFileId > 0 ? true : false;
	}

	// 创建merge文件并将其添加到fileMap中
	public void generateMergeFileId() {
		mergeFileId = incMaxFileId();
		createNewFile(mergeFileId);
	}

	public Item appendToMergeFile(IndexItem indexItem) {
		// 获取旧的item对应的value值
		long fileId = indexItem.getFileId();
		Item item = new Item(fileId, indexItem.getStartPos(),
		        indexItem.getTsTamp());
		LogFile logFile = logFileTable.get(fileId);
		String value = logFile.readValue(item);

		// 计算新的item信息
		String key = indexItem.getKey();
		long tsTamp = indexItem.getTsTamp();

		// 将新的信息写入到merge文件中
		logFile = logFileTable.get(mergeFileId);
		item = logFile.writeItem(key, value, tsTamp);
		return item;
	}

	private long incMaxFileId() {
		return ++fileNumber;
	}

	// 创建新文件，并将其添加到fileMap中
	private void createNewFile(long fileId) {
		String filename = dir + Utility.getPathSep() + fileId + ".log";
		File file = new File(filename);
		logFileTable.put(fileId, new LogFile(filename, fileId));
	}

	// 获取当前可用的文件信息
	private long getNewFileId() {
		Iterator<Entry<Long, LogFile>> iter = logFileTable.entrySet()
		        .iterator();
		while (iter.hasNext()) {
			Entry<Long, LogFile> entry = iter.next();
			if (entry.getValue().getCurLen() == 0) {
				return entry.getKey();
			}
		}
		return -1;
	}
	private String dir;
	private long fileMaxLen;
	private long fileNumber;
	private long activeFileId;
	private long mergeFileId;
	private HashMap<Long, LogFile> logFileTable;
}