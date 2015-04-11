package BitCask;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class TestFileHanlder {
	public TestFileHanlder(String dir, long maxLen, long fileNumber) {
		fileHandler = new BitCask(dir, maxLen, fileNumber);
		testInput = new HashMap<String, String>();
	}

	//测试读取
	public boolean testGet(){
		Iterator<Map.Entry<String, String>> iter = testInput.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = iter.next();
			String str = fileHandler.get(entry.getKey());
			if(str == null){
				System.out.println(entry.getKey());
			}
			if (str != null && str.equals(entry.getValue()) == false) {
				return false;
			}
		}
		return true;
	}

	//测试添加元素
	public boolean testAdd(){
		for(int i = 0; i < 500000; ++i){
			testInput.put(i+"", i+"");
			fileHandler.add(i+"", i+"");
		}
		for(int i = 0; i < 20000; ++i){
			testInput.put(i+"", i+"new");
			fileHandler.add(i+"", i+"new");
		}
		return true;
	}



	HashMap<String, String> testInput;
	BitCask fileHandler;
}

public class Test {

	public static void main(String[] args) {
		TestFileHanlder testFileHandler = new TestFileHanlder("E:\\bitcask", 500 * 1024, 10);
		testFileHandler.testAdd();
		boolean k = testFileHandler.testGet();
		System.out.println(k);
	}
}