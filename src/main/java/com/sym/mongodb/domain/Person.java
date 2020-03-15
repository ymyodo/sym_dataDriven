package com.sym.mongodb.domain;

import lombok.Data;

@Data
public class Person {
	private int good;
	private int bad;
	private String name;
	private boolean isDel;
}
