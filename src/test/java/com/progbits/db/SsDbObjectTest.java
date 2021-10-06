package com.progbits.db;

import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import org.testng.annotations.Test;

/**
 *
 * @author scarr
 */
public class SsDbObjectTest {

    @Test
    public void testSelect() throws ApiException {
        ApiObject objTest = new ApiObject();

        objTest.setString("tableName", "my_test");

        ApiObject objRet = SsDbObjects.createSqlFromFind(null, objTest);

        assert objRet != null;
    }

    @Test
    public void testWhere() throws ApiException {
        ApiObject objTest = new ApiObject();

        objTest.setString("tableName", "my_test");

        objTest.setString("firstName", "Scott Carr");

        ApiObject objRet = SsDbObjects.createSqlFromFind(null, objTest);

        assert objRet != null;

        assert "SELECT * FROM my_test WHERE firstName= ? ".equals(objRet.getString("selectSql"));
    }

    @Test
    public void testWhereMultiple() throws ApiException {
        for (int iCnt = 0; iCnt < 1000; iCnt++) {
            testWhere();
        }
    }

    @Test
    public void testLogicalWhere() throws ApiException {
        ApiObject objTest = new ApiObject();

        objTest.setString("tableName", "my_test");

        objTest.setString("firstName", "Scott Carr");

        SsDbObjects.addLogicalQueryParam("$or", objTest, "lastName", "$like", "MyTest%");
        SsDbObjects.addLogicalQueryParam("$or", objTest, "city", "$like", "MyTest%");
        
        ApiObject objRet = SsDbObjects.createSqlFromFind(null, objTest);
        
        assert objRet != null;
    }
}
