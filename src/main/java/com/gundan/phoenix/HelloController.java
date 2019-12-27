package com.gundan.phoenix;

import static com.gundan.phoenix.db.PhoenixConn.listPhoenix;
import static com.gundan.phoenix.db.PhoenixConn.execute;
import static com.gundan.phoenix.util.MapUtils.create;

import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Lists;
import com.gundan.phoenix.bo.SqlPart;
import com.gundan.phoenix.bo.WherePart;
import com.gundan.phoenix.db.HbaseConn;
import com.gundan.phoenix.util.StringUtils;

@RestController
@RequestMapping("/phb")
public class HelloController {

	@RequestMapping("/hello")
	public String hello() {
		return "hello spring boot";
	}
	
	@PostMapping("/list")
	public Map<String, Object> list(@RequestBody WherePart wp) {
		List<Map<String, String>> res = Lists.newArrayList();
		try {
			res = listPhoenix(wp);
		} catch (Exception e) {
			return create("is_success", "false", "message", e.getMessage());
		}
		return create("is_success", "true", "datas", res, "size", res.size());
	}
	
	@PostMapping("/execute")
	public Map<String, Object> execteSql(@RequestBody SqlPart sp) {
		int ct = 0;
		try {
			ct = execute(sp.getSql(), sp.getArgs());
		} catch (Exception e) {
			return create("is_success", "false", "message", e.getMessage());
		}
		return create("is_success", "true", "count", ct);
	}
	
	@RequestMapping("/scan")
	public Map<String, Object> scan(@RequestParam("table") String table, @RequestParam("key") String key) {
		try {
			if(!HbaseConn.isExists(table)) {
				return create("is_success", "false", "message", "数据表不存在！");
			}
			List<Map<String, Object>> res = HbaseConn.scan(table, StringUtils.EMPTY, key, key + '~');
			return create("is_success", "true", "datas", res, "size", res.size());
		} catch (Exception e) {
			e.printStackTrace();
			return create("is_success", "false", "message", "系统异常！");
		}
	}
}
