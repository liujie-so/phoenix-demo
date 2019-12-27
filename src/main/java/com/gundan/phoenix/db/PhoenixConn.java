package com.gundan.phoenix.db;

import static com.gundan.phoenix.util.MapUtils.create;
import static com.gundan.phoenix.util.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.endsWithIgnoreCase;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.gundan.phoenix.bo.WherePart;
import com.gundan.phoenix.util.MapUtils;
import com.gundan.phoenix.util.PropUtils;
import com.gundan.phoenix.util.StringUtils;

public class PhoenixConn {
	private static final Log log = LogFactory.getLog(PhoenixConn.class);

	private final static String ZK_QUORUM = PropUtils.getValue("hbase.zookeeper.quorum");
	private final static String ZK_ZNODE_PARENT = PropUtils.getValue("zookeeper.znode.parent");
	private static final String DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

	private static Connection conn = null;
	private static final String ORDER_SUFFIX = "_order";

	public static void main(String[] args) {
		List<Map<String, String>> list = listPhoenix("FK_GRAPHS.ZDK_TEST_COPY",
				create("cyzjdm", "110101196005263518"), "ZDK_TEST_CYZJDM", "*");

		for (Map<String, String> map : list) {
			map.entrySet().stream().forEach(f -> System.out.print(f.getKey() + ": " + f.getValue() + " ~ "));
		}
		System.out.println();
		upsert("FK_GRAPHS.XJSBT_GLKY_YPXX_COPY",
				create("id", "43092119900209383X", "zjhm", "43092119900209383X"));
	}

	public static Connection getConnection() {
		try {
			Class.forName(DRIVER);
			if (conn == null || conn.isClosed()) {
				conn = DriverManager.getConnection("jdbc:phoenix:" + ZK_QUORUM + ":" + ZK_ZNODE_PARENT);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * 修改数据
	 * 
	 * @param sql
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public static int execute(String sql, Object[]... args) {
		if (isBlank(sql)) {
			return 0;
		}
		int count = 0;
		try (Connection innerConn = getConnection(); PreparedStatement pstm = innerConn.prepareStatement(sql);) {
			if (ArrayUtils.isNotEmpty(args)) {
				for (Object[] arg : args) {
					for (int i = 0; i < arg.length; i++) {
						if (arg[i] instanceof String) {
							pstm.setString(i + 1, StringUtils.nvl(arg[i]));
						} else if (arg[i] instanceof Date) {
							pstm.setTimestamp(i + 1, new Timestamp(((Date) arg[i]).getTime()));
						} else {
							pstm.setInt(i + 1, Integer.parseInt(StringUtils.nvl(args[i], "0")));
						}
					}
					count += pstm.executeUpdate();
				}
			}
			innerConn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage(), e);
		}

		return count;
	}

	public static int upsert(String sql) {
		return execute(sql, ArrayUtils.EMPTY_OBJECT_ARRAY);
	}

	/**
	 * 插入数据
	 * 
	 * @param tableName
	 * @param map
	 * @return
	 * @throws Exception
	 */
	public static int upsert(String tableName, Map<String, ?> map) {
		String columns = map.keySet().stream().collect(Collectors.joining(","));
		String values = map.keySet().stream().map(f -> "?").collect(Collectors.joining(","));

		return execute("upsert into " + tableName + "(" + columns + ") values(" + values + ")", map.values()
				.toArray());
	}

	public static int batchUpsert(String tableName, List<Map<String, ?>> maps) {
		if (CollectionUtils.isEmpty(maps)) {
			return 0;
		}
		List<String> cols = maps.get(0).keySet().stream().collect(Collectors.toList());
		List<Object[]> collect = maps.stream().map(f -> {
			Object[] args = new Object[cols.size()];
			for (int i = 0; i < cols.size(); i++) {
				args[i] = f.get(cols.get(i));
			}
			return args;
		}).collect(Collectors.toList());

		String columns = maps.get(0).keySet().stream().collect(Collectors.joining(","));
		String values = maps.get(0).keySet().stream().map(f -> "?").collect(Collectors.joining(","));

		return execute("upsert into " + tableName + "(" + columns + ") values(" + values + ")",
				collect.toArray(new Object[][] {}));
	}

	public static List<Map<String, String>> listPhoenix(String tableName, Map<String, Object> whereRegular,
			String index, String... columns) {
		StringBuilder sql = new StringBuilder("select ");
		/**
		 * 拼接索引
		 */
		if (isNotBlank(index)) {
			sql.append("/*+ index(").append(tableName).append(" ").append(index).append(")*/ ");
		}
		String outputs = "*";
		if(ArrayUtils.isNotEmpty(columns)) {
			outputs = Arrays.asList(columns).stream().collect(Collectors.joining(","));
		}
		sql.append(outputs).append(" from ").append(tableName).append(" where 1=1 ");
		/**
		 * 参数列表
		 */
		List<Object> params = Lists.newArrayList();
		if (MapUtils.isNotEmpty(whereRegular)) {
			String orderPart = "";
			for (Map.Entry<String, Object> regular : whereRegular.entrySet()) {
				if (isBlank(regular.getValue())) {
					continue;
				}
				if (endsWithIgnoreCase(regular.getKey(), ORDER_SUFFIX)) {
					orderPart = " order by " + StringUtils.removeEnd(regular.getKey(), ORDER_SUFFIX) + " "
							+ regular.getValue();
					continue;
				}
//				String name = regular.getKey().split("[^\\w]")[0];
				sql.append(" and ").append(regular.getKey()).append("?");
				params.add(regular.getValue());
			}
			sql.append(orderPart);
		}
		return listPhoenix(sql.toString(), params.toArray());
	}

	public static List<Map<String, String>> listPhoenix(String sql, Object[] args) {

		System.out.println("=========打印 sql: " + sql);

		List<String> outputColumns = Lists.newArrayList();
		List<Map<String, String>> res = Lists.newArrayList();
		try (Connection innerConn = getConnection();
				PreparedStatement pstm = createStatement(innerConn, sql, args);
				ResultSet rs = pstm.executeQuery();) {
			ResultSetMetaData metaData = rs.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				outputColumns.add(metaData.getColumnName(i).toLowerCase());
			}
			while (rs.next()) {
				Map<String, String> data = Maps.newHashMap();
				for (int i = 0; i < outputColumns.size(); i++) {
					data.put(outputColumns.get(i), rs.getString(outputColumns.get(i)));
				}
				res.add(data);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return res;
	}
	
	public static List<Map<String, String>> listPhoenix(WherePart wp) {
		return listPhoenix(wp.getTable(), wp.getParams(), wp.getIndex(), wp.getColumns());
	}

	public static PreparedStatement createStatement(Connection c, String sql, Object... args)
			throws SQLException {
		PreparedStatement ps = c.prepareStatement(sql);
		if (ArrayUtils.isNotEmpty(args)) {
			for (int i = 0; i < args.length; i++) {
				if (args[i] instanceof String) {
					ps.setString(i + 1, StringUtils.nvl(args[i]));
				} else if (args[i] instanceof Date) {
					ps.setTimestamp(i + 1, new Timestamp(((Date) args[i]).getTime()));
				} else {
					ps.setInt(i + 1, Integer.parseInt(StringUtils.nvl(args[i], "0")));
				}
			}
		}
		return ps;
	}

}
