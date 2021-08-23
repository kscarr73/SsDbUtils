package com.progbits.db;

import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import static com.progbits.db.SsDbUtils.insertObjectWithKey;
import static com.progbits.db.SsDbUtils.insertObjectWithStringKey;
import static com.progbits.db.SsDbUtils.querySqlAsApiObject;
import static com.progbits.db.SsDbUtils.updateObject;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A set of tools to use ApiObject with Databases
 * 
 * @author scarr
 */
public class SsDbObjects {
	public static void addQueryParam(ApiObject find, String fieldName, String operator, Object value) {
		if (operator != null) {
			ApiObject objOperator = new ApiObject();
			
			objOperator.put(operator, value);
			
			find.setObject(fieldName, objOperator);
		} else {
			find.put(fieldName, value);
		}
	}
	
	public static ApiObject find(Connection conn, ApiObject searchObject) throws ApiException {
		ApiObject selectSql = createSqlFromFind(searchObject);
		
		if (selectSql.isSet("selectSql")) {
			String strSql = selectSql.getString("selectSql");
			String strCount = selectSql.getString("countSql");
			
			selectSql.remove("selectSql");
			selectSql.remove("countSql");
			
			ApiObject objCount = SsDbUtils.querySqlAsApiObject(conn, strCount, selectSql);
			
			ApiObject objRet = SsDbUtils.querySqlAsApiObject(conn, strSql, selectSql);
			
			if (objCount.containsKey("root")) {
				objRet.setLong("total", objCount.getLong("root[0].total"));
			}
			
			return objRet;
		} else {
			throw new ApiException("SQL Was not created Propertly");
		}
	}
	
	public static ApiObject createSqlFromFind(ApiObject find) throws ApiException {
		StringBuilder sbSql = new StringBuilder();
		StringBuilder sbCount = new StringBuilder();
		
		if (find.isEmpty() || !find.containsKey("tableName")) {
			throw new ApiException("tableName IS required");
		}
		
		String tableName = find.getString("tableName");
		
		if (!find.isSet("fields")) {
			sbSql.append("SELECT * FROM ").append(tableName);
		} else {
			sbSql.append("SELECT ");
			boolean bFirst = true;
			
			for (String field : find.getStringArray("fields")) {
				if (bFirst) {
					bFirst = false;
				} else {
					sbSql.append(",");
				}
				
				sbSql.append(field);
			}
		}
		
		sbCount.append("SELECT COUNT(*) AS total FROM ").append(tableName);
		
		ApiObject selectObj = createWhereFromFind(find);
		
		if (selectObj.isSet("whereSql")) {
			sbSql.append(" ").append(selectObj.get("whereSql"));
			sbCount.append(" ").append(selectObj.get("whereSql"));
			
			selectObj.remove("whereSql");
		}
		
		if (find.isSet("orderBy")) {
			sbSql.append(" ORDER BY ");
			boolean bFirst = true;
			
			for (String fieldName : find.getStringArray("orderBy")) {
				if (bFirst) {
					bFirst = false;
				} else {
					sbSql.append(",");
				}
				
				boolean ascending = true;
				
				if (fieldName.startsWith("+")) {
					fieldName = fieldName.substring(1);
				} else if (fieldName.startsWith("-")) {
					fieldName = fieldName.substring(1);
					ascending = false;
				}
				
				sbSql.append(fieldName);
				
				if (ascending) {
					sbSql.append(" asc");
				} else {
					sbSql.append(" desc");
				}
			}
		}
		
		if (find.containsKey("length")) {
			sbSql.append(" LIMIT ").append(find.getInteger("length"));
		}
		
		if (find.containsKey("start")) {
			sbSql.append(" OFFSET ").append(find.getInteger("start"));
		}
		
		selectObj.setString("selectSql", sbSql.toString());
		selectObj.setString("countSql", sbCount.toString());
				
		return selectObj;
	}
	
	private static final List<String> controlFields = new ArrayList<String>(Arrays.asList(new String[] { "tableName", "fields", "or", "start", "length", "orderBy" }));
	
	public static ApiObject createWhereFromFind(ApiObject find) throws ApiException {
		StringBuilder sbWhere = new StringBuilder();
		ApiObject retObj = new ApiObject();
		
		boolean bFirst = true;
		
		for (String key : find.keySet()) {
			if (!controlFields.contains(key)) {
				if (bFirst) {
					bFirst = false;
				} else {
					if (find.isSet("or")) {
						sbWhere.append(" OR ");
					} else {
						sbWhere.append(" AND ");
					}
				}
				
				if (find.getType(key) == ApiObject.TYPE_OBJECT) {
					if (find.getObject(key).containsKey("$eq")) {
						sbWhere.append(key).append("=").append(":").append(key).append(" ");
						retObj.put(key, find.getObject(key).get("$eq"));
					} else if (find.getObject(key).containsKey("$gt")) {
						sbWhere.append(key).append(">").append(":").append(key).append(" ");
						retObj.put(key, find.getObject(key).get("$gt"));
					} else if (find.getObject(key).containsKey("$lt")) {
						sbWhere.append(key).append("<").append(":").append(key).append(" ");
						retObj.put(key, find.getObject(key).get("$lt"));
					} else if (find.getObject(key).containsKey("$gte")) {
						sbWhere.append(key).append(">=").append(":").append(key).append(" ");
						retObj.put(key, find.getObject(key).get("$gte"));
					} else if (find.getObject(key).containsKey("$lte")) {
						sbWhere.append(key).append("<=").append(":").append(key).append(" ");
						retObj.put(key, find.getObject(key).get("$lte"));
					} else if (find.getObject(key).containsKey("$in")) {
						sbWhere.append(key).append(" IN (").append(":").append(key).append(" ) ");
						retObj.put(key, find.getObject(key).get("$in"));
					} else if (find.getObject(key).containsKey("$like")) {
						sbWhere.append(key).append(" LIKE ").append(":").append(key).append(" ");
						retObj.put(key, find.getObject(key).get("$like"));
					} else if (find.getObject(key).containsKey("$reg")) {
						sbWhere.append(key).append(" REGEXP ").append(":").append(key).append(" ");
						retObj.put(key, find.getObject(key).get("$reg"));
					}
				} else {
					sbWhere.append(key).append("=").append(":").append(key);
					retObj.put(key, find.get(key));
				}
			}
		}
		
		if (sbWhere.length() > 0) {
			retObj.setString("whereSql", "WHERE " + sbWhere.toString());
		}
		
		return retObj;
	}
	
	/**
	 * Upsert an Object to the database, given an id field
	 *
	 * @param conn Connection to use for processing the Object
	 * @param tableName The table for the information being inserted
	 * @param idField The field used as an ID for the table
	 * @param obj The object to insert or update
	 *
	 * @return The Object that was created in the Database
	 *
	 * @throws ApiException If the ROW that was updated or inserted is not
	 * returned
	 */
	public static ApiObject upsertWithInteger(Connection conn, String tableName, String idField, ApiObject obj) throws ApiException {
		Integer idValue = null;

		if (obj.isSet(idField)) {
			idValue = obj.getInteger(idField);
			
			updateObject(conn, createObjectUpdateSql(tableName, idField, obj), obj);
		} else {  // Perform Insert
			idValue = insertObjectWithKey(conn, createObjectInsertSql(tableName, idField, obj), new String[] { idField }, obj);
		}

		ApiObject searchObj = new ApiObject();
		searchObj.setInteger(idField, idValue);

		ApiObject retObj = querySqlAsApiObject(conn, "SELECT * FROM " + tableName + " WHERE " + idField + "=:" + idField, searchObj);

		if (retObj.isSet("root")) {
			return retObj.getList("root").get(0);
		} else {
			throw new ApiException("ROW Was Not returned for ID: " + idValue);
		}
	}
	
	/**
	 * Upsert an Object to the database, given an id field
	 *
	 * @param conn Connection to use for processing the Object
	 * @param tableName The table for the information being inserted
	 * @param idField The field used as an ID for the table
	 * @param obj The object to insert or update
	 *
	 * @return The Object that was created in the Database
	 *
	 * @throws ApiException If the ROW that was updated or inserted is not
	 * returned
	 */
	public static ApiObject upsertWithString(Connection conn, String tableName, String idField, ApiObject obj) throws ApiException {
		String idValue = null;

		if (obj.isSet(idField)) {
			idValue = obj.getString(idField);
			
			updateObject(conn, createObjectUpdateSql(tableName, idField, obj), obj);
		} else {  // Perform Insert
			idValue = insertObjectWithStringKey(conn, createObjectInsertSql(tableName, idField, obj), new String[] { idField }, obj);
		}

		ApiObject searchObj = new ApiObject();
		searchObj.setString(idField, idValue);

		ApiObject retObj = querySqlAsApiObject(conn, "SELECT * FROM " + tableName + " WHERE " + idField + "=:" + idField, searchObj);

		if (retObj.isSet("root")) {
			return retObj.getList("root").get(0);
		} else {
			throw new ApiException("ROW Was Not returned for ID: " + idValue);
		}
	}

	private static String createObjectUpdateSql(String tableName, String idField, ApiObject obj) throws ApiException {
		if (obj.isEmpty()) {
			throw new ApiException("Object MUST not be empty");
		}
		
		StringBuilder sb = new StringBuilder();

		sb.append("UPDATE ");
		sb.append(tableName);
		sb.append(" SET ");
		
		boolean bFirst = true;
		
		for (String fldName : obj.keySet()) {
			if (bFirst) {
				bFirst = false;
			} else {
				sb.append(",");
			}
			
			sb.append(fldName).append("=");
			sb.append(":").append(fldName);
		}
		
		sb.append(" WHERE ");
		sb.append(idField);
		sb.append("=:");
		sb.append(idField);
		
		return sb.toString();
	}
	
	private static String createObjectInsertSql(String tableName, String idField, ApiObject obj) throws ApiException {
		if (obj.isEmpty()) {
			throw new ApiException("Object MUST not be empty");
		}
		
		StringBuilder sb = new StringBuilder();
		StringBuilder sbFields = new StringBuilder();
		
		sb.append("INSERT INTO ");
		sb.append(tableName);
		sb.append(" (");
		
		boolean bFirst = true;
		
		for (String fldName : obj.keySet()) {
			if (bFirst) {
				bFirst = false;
			} else {
				sb.append(",");
				sbFields.append(",");
			}
			
			sb.append(fldName);
			sbFields.append(":").append(fldName);
		}
		
		sb.append(") VALUES (");
		sb.append(sbFields);
		sb.append(")");
		
		return sb.toString();
	}
}
