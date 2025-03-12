package com.progbits.db;

import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import java.time.OffsetDateTime;
import org.testng.annotations.Test;

/**
 *
 * @author Kevin.Carr
 */
public class SsObjectWhereTest {
    
    @Test
    public void testWhere() throws ApiException {
        ApiObject objTest = new ApiObject();
        
        objTest.createObject("program_name")
                .setBoolean("$case", false)
                .setString("$like", "%Community%");
        
        ApiObject objResp = SsDbObjects.createWhereFromFind(objTest);
        
        assert objResp != null;
    }
    
    @Test
    public void testWhere2() throws ApiException {
        ApiObject objTest = new ApiObject();
        
        objTest.createObject("createddt")
                .setDateTime("$gte", OffsetDateTime.now().minusDays(2))
                .setDateTime("$lte", OffsetDateTime.now());
                
        ApiObject objResp = SsDbObjects.createWhereFromFind(objTest);
        
        assert objResp != null;
    }
}
