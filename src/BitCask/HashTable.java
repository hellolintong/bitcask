package BitCask;

import java.util.HashMap;

//元素结构体
class Item{
	Item(long fileId, long startPos, long tsTamp){
		this.fileId = fileId;
		this.startPos = startPos;
		this.tsTamp = tsTamp;
	}
	public long getFileId() {
		return fileId;
	}
	public void setFileId(long fileId) {
		this.fileId = fileId;
	}

	public long getStartPos() {
		return startPos;
	}
	public void setStartPos(long startPos) {
		this.startPos = startPos;
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
}

//哈希表类
class Hash{
	Hash(){
		hashTable = new HashMap<String, Item>();
		backHashTable = new HashMap<String, Item>();
	}
	public void swap(){
		HashMap<String, Item> temp = hashTable;
		hashTable = backHashTable;
		backHashTable = temp;
		backHashTable.clear();
	}
	
	public void backItem(String key, Item item){
		backHashTable.put(key, item);
	}
	public void addItem(String key, Item item){
		hashTable.put(key, item);
	}
	public Item getItem(String key){
		return hashTable.get(key);
	}
	private HashMap<String, Item> hashTable;//主哈希表
	private HashMap<String, Item> backHashTable;//用于merge操作时备份用的哈希表
}
