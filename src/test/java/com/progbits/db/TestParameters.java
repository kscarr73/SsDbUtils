package com.progbits.db;

import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

/**
 *
 * @author scarr
 */
public class TestParameters {

    @Test
    public void testParameterCall() throws ApiException {
        String sql = "INSERT INTO tableName (This, That, Other) VALUES (:This, :That, :Other)";
        
        List<Object> params = new ArrayList<>();
        
        ApiObject args = new ApiObject();
        
        args.setString("This", "Something");
        args.setInteger("That", 12);
        args.setDouble("Other", 12.12);
        
        String lclSql = SsDbUtils.gatherParamsForSql(sql, params, args);
        
        assert lclSql != null;
        assert "INSERT INTO tableName (This, That, Other) VALUES (?, ?, ?)".equals(lclSql);
        
        assert params.get(1).equals(12);
    }
    
    @Test
    public void testStringArrayCall() throws ApiException {
        String sql = "SELECT * FROM table WHERE id IN (:MyValue) AND name=:MyName";
        
        List<Object> params = new ArrayList<>();
        
        ApiObject args = new ApiObject();
        
        args.setString("MyName", "Something");
        
        args.createStringArray("MyValue");
        args.getStringArray("MyValue").add("This");
        args.getStringArray("MyValue").add("That");
        args.getStringArray("MyValue").add("Other");
        
        String lclSql = SsDbUtils.gatherParamsForSql(sql, params, args);
        
        assert lclSql != null;
        assert "SELECT * FROM table WHERE id IN (?,?,?) AND name=?".equals(lclSql);
        
        assert params.get(1).equals("That");
    }
    
    @Test
    public void testIntegerArrayCall() throws ApiException {
        String sql = "SELECT * FROM table WHERE id IN (:MyValue) AND name=:MyName";
        
        List<Object> params = new ArrayList<>();
        
        ApiObject args = new ApiObject();
        
        args.setString("MyName", "Something");
        
        args.createIntegerArray("MyValue");
        args.getIntegerArray("MyValue").add(2);
        args.getIntegerArray("MyValue").add(13);
        args.getIntegerArray("MyValue").add(15);
        
        String lclSql = SsDbUtils.gatherParamsForSql(sql, params, args);
        
        assert lclSql != null;
        assert "SELECT * FROM table WHERE id IN (?,?,?) AND name=?".equals(lclSql);
        
        assert params.get(1).equals(13);
    }
    
    @Test(expectedExceptions = { ApiException.class })
    public void testArgNotExist() throws ApiException {
        String sql = "SELECT * FROM table WHERE id IN (:MyValue) AND name=:MyName2";
        
        List<Object> params = new ArrayList<>();
        
        ApiObject args = new ApiObject();
        
        args.setString("MyName", "Something");
        
        args.createIntegerArray("MyValue");
        args.getIntegerArray("MyValue").add(2);
        args.getIntegerArray("MyValue").add(13);
        args.getIntegerArray("MyValue").add(15);
        
        String lclSql = SsDbUtils.gatherParamsForSql(sql, params, args);
        
        assert lclSql != null;
        assert "SELECT * FROM table WHERE id IN (?,?,?) AND name=?".equals(lclSql);
        
        assert params.get(1).equals(13);
    }

}
