package chapter02;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @time: 2020-10-04 21:15
 * @author: likunlun
 * @description: 对象流测试
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person implements Serializable {
	public static final long serialVersionUID = 1111111111L;
	private String name;
	private int age;
	private String sex;

	public Person(String name, int age) {
		this.name = name;
		this.age = age;
	}
}
