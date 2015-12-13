package com.yangmao;

public class Test {
	public static void main(String[] args) {
		String test = "hello,wow, ha";
		String[] group = test.split(",");
		System.out.println(group[1]);
		if(group.length == 3){
			System.out.println("yes");
		}
	}
}
