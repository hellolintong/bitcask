package BitCask;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class Test {
	public boolean TestFileHandler(String dir, String activeFile){
		FileHandler handler = new FileHandler(dir, activeFile,
		        1024 * 1024 * 5);
		HashTable hashTable = new HashTable();
		HashMap<String, String> testInput = new HashMap<String, String>();
		testInput.put("abc", "你好，abc");
		testInput.put("林通", "你好，林通");
		testInput.put("林通", "你好，林通哥哥");
		testInput.put("哈哈", "你好， 哈哈");

		//first add elem into log file and hashtable
		Iterator<Map.Entry<String, String>> iter = testInput.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<String, String> entry = iter.next();
			Item item = handler.append(entry.getKey(), entry.getValue());
			hashTable.addItem(entry.getKey(), item);
		}

		//then read them out and check if they are equal to what we want
		iter = testInput.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<String, String> entry = iter.next();
			Item item = hashTable.getItem(entry.getKey());
			String str = handler.getValue(item);
			if(str.equals(entry.getValue()) == false){
				return false;
			}
		}
		return true;
	}
	public static void main(String[] args) {
		Test test = new Test();
		boolean k = test.TestFileHandler("E:\\bitcask", "222.log");
		System.out.println(k);
	}
}