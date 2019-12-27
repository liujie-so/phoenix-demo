package com.gundan.phoenix.db;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.gundan.phoenix.util.PropUtils;
import com.gundan.phoenix.util.StringUtils;

public class HbaseConn {
	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set(HConstants.ZOOKEEPER_QUORUM, PropUtils.getValue("hbase.zookeeper.quorum"));
		configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, PropUtils.getValue("zookeeper.znode.parent"));
	}
	private static Connection conn = null;

	public static Connection createConn() throws IOException {
		if (conn == null || conn.isClosed()) {
			conn = ConnectionFactory.createConnection(configuration);
		}
		return conn;
	}
	
	public static List<Map<String, Object>> scan(String tableName, String columns, String startRow,
			String stopRow) throws IOException {
		Table table = null;
		ResultScanner scanner = null;
		try {
			createConn();
			table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			if (StringUtils.isNotBlank(columns)) {
				Arrays.stream(StringUtils.split(columns, StringUtils.COMMA)).forEach(c -> {
					if (StringUtils.contains(c, ":")) {
						String[] arr = c.split(":");
						scan.addColumn(arr[0].getBytes(), arr[1].getBytes());
					} else {
						scan.addFamily(c.getBytes());
					}
				});
			}
			scan.setStartRow(startRow.getBytes());
			scan.setStopRow(stopRow.getBytes());
			scanner = table.getScanner(scan);
			
			return Lists.newArrayList(scanner.iterator()).stream().map(HbaseConn::transferResult)
					.collect(Collectors.toList());
			
		} finally {
			closeX(table, scanner, conn);
		}
		
	}
	
	public static Map<String, Object> transferResult(Result result) {
		Map<String, Object> res = Maps.newHashMap();
		res.put("id", Bytes.toString(result.getRow()));
		for (Cell cell : result.rawCells()) {
			String key = Bytes.toString(CellUtil.cloneQualifier(cell));
			String value = Bytes.toString(CellUtil.cloneValue(cell));
			res.put(key, value);
		}
		return res;
	}
	
	public static boolean isExists(String tableName) throws IOException {
		Admin admin = null;
		try {
			createConn();
			admin = conn.getAdmin();
			return admin.tableExists(TableName.valueOf(tableName));
		} finally {
			closeX(admin, conn);
		}
		
	}
	
	public static void closeX(Closeable... cs) {
		if (ArrayUtils.isNotEmpty(cs)) {
			try {
				for (Closeable c : cs) {
					if (c != null) {
						c.close();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}