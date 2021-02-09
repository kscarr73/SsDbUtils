/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.progbits.db;

/**
 *
 * @author scarr
 */
public class Tuple<T1, T2> {
	protected T1 t1;
	protected T2 t2;
	
	public T1 getFirst() {
		return t1;
	}
	
	public void setFirst(T1 value) {
		t1 = value;
	}
	
	public T2 getSecond() {
		return t2;
	}
	
	public void setSecond(T2 value) {
		t2 = value;
	}
}
