package com.sist.regex;

import java.io.Serializable;

public class MyVO implements Serializable{//메모리에서 필요
	private String ip;
	private int count;
	
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	
}
