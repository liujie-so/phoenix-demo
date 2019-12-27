package com.gundan.phoenix.bo;

import java.util.Arrays;

public class SqlPart {
	private String sql;
	private Object[] args;
	public SqlPart(String sql, Object[] args) {
		this.sql = sql;
		this.args = args;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public Object[] getArgs() {
		return args;
	}
	public void setArgs(Object[] args) {
		this.args = args;
	}
	@Override
	public String toString() {
		return "SqlPart [sql=" + sql + ", args=" + Arrays.toString(args) + "]";
	}
}
