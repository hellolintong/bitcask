package BitCask;

class BitCask {
	public BitCask(String dir, long maxFileLen, long fileNumber) {
		this.logFileHandler = new LogFileHandler(dir, maxFileLen, fileNumber);
		this.indexFileHandler = new IndexFileHandler(dir);
		this.itemHashTable = new Hash();
		if(this.logFileHandler.initActiveFile() == false){
			compress();
			this.logFileHandler.initActiveFile();
		}
	}
	
	// bitcast对外开放的接口
	// 将key与value加入到bitcask中
	public boolean add(String key, String value) {
		if (key == null || value == null) {
			return false;
		}
		
		Item item = logFileHandler.append(key, value);
		//如果item 为null 说明活跃文件的长度超过了最大长度，需要压缩操作
		if(item == null){
			compress();
			item = logFileHandler.append(key, value);
		}
		itemHashTable.addItem(key, item);
		indexFileHandler.addIndex(Utility.itemToIndexItem(item, key));
		return true;
	}

	// 获取key对应的value值
	public String get(String key) {
		if (key == null) {
			return null;
		}
		Item item = itemHashTable.getItem(key);
		return logFileHandler.read(item);
	}
	
	
	private void compress(){
		//先创建merge文件
		logFileHandler.generateMergeFileId();

		// 开始压缩index文件,并将内容写入merge 问价以及index file与哈希表中
		indexFileHandler.compress(logFileHandler, itemHashTable);
		
		//清空其他文件
		logFileHandler.clearLogFile();
		
		//重新生成active file
		logFileHandler.initActiveFile();
		
		// 更新itemHashTabel
		itemHashTable.swap();	
		
	}

	private IndexFileHandler indexFileHandler;// index文件处理类
	private Hash itemHashTable;// 元素哈希表（用于记录key在文件中的信息）
	private LogFileHandler logFileHandler;//日志文件处理类
}