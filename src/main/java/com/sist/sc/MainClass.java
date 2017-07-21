package com.sist.sc;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MainClass {
  public static void main(String[] arg)
  {
	  ApplicationContext app=
			  new ClassPathXmlApplicationContext("app.xml");
  }
}
