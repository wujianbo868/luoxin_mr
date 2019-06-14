package com.wujianbo.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.runner.BaseRunner;
import com.aliyun.odps.udf.local.runner.UDTFRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class KeyValueTest {

	@Test
	public void testKeyValue() throws UDFException, LocalRunException {
		BaseRunner runner = new UDTFRunner(null, "com.wujianbo.udtf.KeyValue");
		runner.feed(new Object[] { "one=wujianbo,two=luoxin", ",", "=", "two"});
		List<Object[]> out = runner.yield();
		Assert.assertEquals("luoxin", out.get(0)[0]);
	}
}
