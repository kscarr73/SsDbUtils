package com.progbits.db;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.annotations.Test;

/**
 *
 * @author scarr
 */
public class TestSqlPattern {
	@Test
	public void testPattern() throws Exception {
		Pattern pattern = Pattern.compile("(:(.[^\\s,);]*))", Pattern.MULTILINE);
		
		String strSQL = "INSERT INTO something (firstName, lastName, birthDate) \nVALUES (:firstName, :lastName, :birtDate)";
		
		Matcher matcher = pattern.matcher(strSQL);
		
		List<String> foundItems = new ArrayList<>();
		
		while (matcher.find()) {
			foundItems.add(matcher.group(2));
		}
		
		assert foundItems.size() > 0;
	}
}
