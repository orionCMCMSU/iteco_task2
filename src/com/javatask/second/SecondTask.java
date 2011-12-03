/*
 * author: Taranov V.V.
 * mailto: taranov.vv@gmail.com
 */

package com.javatask.second;

public class SecondTask {

	public static void main(String[] args) {

		JDBCTest task = new JDBCTest(); 
		
		task.testStatement();
		task.testPreparedStatement();
		task.testTransaction();
		
		task.closeConnection();
	}
}
