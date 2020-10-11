package chapter02;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @time: 2020-10-04 21:19
 * @author: likunlun
 * @description: 对象流测试
 */
public class ObjectInputOutStreamTest {

	/**
	 * 序列化：将内存中的java对象保存到磁盘中或者通过网络传输出去
	 *
	 * @throws Exception
	 */
	@Test
	public void testObjectOutputStream() throws Exception {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("object.dat"));
		oos.writeObject("我爱北京天安门");
		oos.flush(); //刷新操作
		Person person = new Person("xiaolun", 18);
		oos.writeObject(person);
		oos.flush();
		oos.close();
	}

	/**
	 * 反序列化：将磁盘文件中的对象还原成内存中的一个java对象
	 *
	 * @throws Exception
	 */
	@Test
	public void testObjectInputStream() throws Exception {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream("object.dat"));
		Object in = ois.readObject();
		String str = (String)in;

		Person person= (Person)ois.readObject();
		System.out.println(str);
		System.out.println(person);
		ois.close();
	}
}
