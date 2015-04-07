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
class HashTable{
	HashTable(){
		hashTable = new HashMap<String, Item>();
	}
	public void addItem(String key, Item item){
		hashTable.put(key, item);
	}
	public Item getItem(String key){
		return hashTable.get(key);
	}
	private HashMap<String, Item> hashTable;
}
