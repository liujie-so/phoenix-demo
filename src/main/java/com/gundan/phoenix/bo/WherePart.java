package com.gundan.phoenix.bo;

import java.util.Arrays;
import java.util.Map;

public class WherePart {
	private String table;
	private String index;
	private String[] columns;
	private Map<String, Object> params;
	
	public WherePart(String table, String index, String[] columns, Map<String, Object> params) {
		this.table = table;
		this.index = index;
		this.columns = columns;
		this.params = params;
	}
	
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getIndex() {
		return index;
	}
	public void setIndex(String index) {
		this.index = index;
	}
	public String[] getColumns() {
		return columns;
	}
	public void setColumns(String[] columns) {
		this.columns = columns;
	}
	public Map<String, Object> getParams() {
		return params;
	}
	public void setParams(Map<String, Object> params) {
		this.params = params;
	}
	@Override
	public String toString() {
		return "WherePart [table=" + table + ", index=" + index + ", columns=" + Arrays.toString(columns)
				+ ", params=" + params + "]";
	}
	
}
