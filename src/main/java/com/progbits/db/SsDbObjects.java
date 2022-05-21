package com.progbits.db;

import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import static com.progbits.db.SsDbUtils.insertObjectWithKey;
import static com.progbits.db.SsDbUtils.insertObjectWithStringKey;
import static com.progbits.db.SsDbUtils.querySqlAsApiObject;
import static com.progbits.db.SsDbUtils.updateObject;
import java.sql.Connection;
import java.sql.SQLException;
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

    public static void addLogicalQueryParam(String logicalName, ApiObject find, String fieldName, String operator, Object value) {
        ApiObject objField = new ApiObject();

        if (!find.containsKey(logicalName)) {
            find.createList(logicalName);
        }

        if (operator != null) {
            ApiObject objOperator = new ApiObject();

            objOperator.put(operator, value);

            objField.setObject(fieldName, objOperator);

            find.getList(logicalName).add(objField);
        } else {
            objField.put(fieldName, value);

            find.getList(logicalName).add(objField);
        }
    }

    public static ApiObject find(Connection conn, ApiObject searchObject) throws ApiException {
        ApiObject selectSql = createSqlFromFind(conn, searchObject);

        if (selectSql.isSet("selectSql")) {
            String strSql = selectSql.getString("selectSql");
            String strCount = selectSql.getString("countSql");
            List<Object> params = (List<Object>) selectSql.get("params");

            selectSql.remove("selectSql");
            selectSql.remove("countSql");

            long lStart = System.currentTimeMillis();

            ApiObject objCount = SsDbUtils.querySqlAsApiObject(conn, strCount, params.toArray());

            long lCountTime = System.currentTimeMillis() - lStart;

            lStart = System.currentTimeMillis();

            ApiObject objRet = SsDbUtils.querySqlAsApiObject(conn, strSql, params.toArray());

            objRet.setLong("sqlTime", System.currentTimeMillis() - lStart);
            objRet.setLong("sqlCount", lCountTime);
            if (objCount.containsKey("root")) {
                objRet.setLong("total", objCount.getLong("root[0].total"));
            }

            return objRet;
        } else {
            throw new ApiException("SQL Was not created Propertly");
        }
    }

    public static ApiObject createSqlFromFind(Connection conn, ApiObject find) throws ApiException {
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

            sbSql.append(" FROM ").append(tableName);
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

        if (find.isSet("orderBy")) {
            String dbType = null;

            try {
                if (conn != null) {
                    dbType = conn.getMetaData().getDatabaseProductName();
                }
            } catch (SQLException sqx) {
                dbType = "MySQL";
            }

            if ("Microsoft SQL Server".equals(dbType)) {
                if (find.containsKey("start")) {
                    sbSql.append(" OFFSET ")
                            .append(find.getInteger("start"))
                            .append(" ROWS ");
                }

                if (find.containsKey("length")) {
                    sbSql.append(" FETCH NEXT ")
                            .append(find.getInteger("length"))
                            .append(" ROWS ONLY ");
                }
            } else if (dbType.equals("MySQL") || dbType.equals("PostgreSQL")) {
                if (find.containsKey("length")) {
                    sbSql.append(" LIMIT ").append(find.getInteger("length"));
                }

                if (find.containsKey("start")) {
                    sbSql.append(" OFFSET ").append(find.getInteger("start"));
                }
            }
        }

        selectObj.setString("selectSql", sbSql.toString());
        selectObj.setString("countSql", sbCount.toString());

        return selectObj;
    }

    private static final List<String> controlFields = new ArrayList<String>(Arrays.asList(new String[]{"tableName", "fields", "$or", "$and", "start", "length", "orderBy"}));
    private static final List<String> controlFieldsLogical = new ArrayList<String>(Arrays.asList(new String[]{"$or", "$and"}));

    public static ApiObject createWhereFromFind(ApiObject find) throws ApiException {
        StringBuilder sbWhere = new StringBuilder();
        ApiObject retObj = new ApiObject();
        List<Object> params = new ArrayList<>();

        boolean bFirst = true;

        for (String key : find.keySet()) {
            if (!controlFields.contains(key)) {
                if (bFirst) {
                    bFirst = false;
                } else {
                    sbWhere.append(" AND ");
                }

                applyWhereField(find, key, sbWhere, params);
            } else if (controlFieldsLogical.contains(key)) {
                if (bFirst) {
                    bFirst = false;
                } else {
                    sbWhere.append(" AND ");
                }

                if ("$or".equals(key)) {
                    processWhereLogical(find.getList(key), " OR ", sbWhere, params);
                } else if ("$and".equals(key)) {
                    processWhereLogical(find.getList(key), " AND ", sbWhere, params);
                }
            }
        }

        if (sbWhere.length() > 0) {
            retObj.setString("whereSql", "WHERE " + sbWhere.toString());
        }

        retObj.put("params", params);

        return retObj;
    }

    private static void processWhereLogical(List<ApiObject> subject, String logicalType, StringBuilder sbWhere, List<Object> params) {
        sbWhere.append(" ( ");
        boolean bFirst = true;

        for (ApiObject row : subject) {
            for (var key : row.keySet()) {
                if (bFirst) {
                    bFirst = false;
                } else {
                    sbWhere.append(logicalType);
                }

                if (controlFieldsLogical.contains(key)) {
                    if ("$or".equals(key)) {
                        processWhereLogical(row.getList(key), " OR ", sbWhere, params);
                    } else if ("$and".equals(key)) {
                        processWhereLogical(row.getList(key), " AND ", sbWhere, params);
                    }
                } else {
                    applyWhereField(row, key, sbWhere, params);
                }
            }
        }

        sbWhere.append(" ) ");
    }

    private static void applyWhereField(ApiObject find, String key, StringBuilder sbWhere, List<Object> params) {
        if (find.getType(key) == ApiObject.TYPE_OBJECT) {
            if (find.getObject(key).containsKey("$eq")) {
                sbWhere.append(key).append("=").append(" ?").append(" ");
                params.add(find.getObject(key).get("$eq"));
            } else if (find.getObject(key).containsKey("$gt")) {
                sbWhere.append(key).append(">").append(" ?").append(" ");
                params.add(find.getObject(key).get("$gt"));
            } else if (find.getObject(key).containsKey("$lt")) {
                sbWhere.append(key).append("<").append(" ?").append(" ");
                params.add(find.getObject(key).get("$lt"));
            } else if (find.getObject(key).containsKey("$gte")) {
                sbWhere.append(key).append(">=").append(":").append(key).append(" ");
                params.add(find.getObject(key).get("$gte"));
            } else if (find.getObject(key).containsKey("$lte")) {
                sbWhere.append(key).append("<=").append(":").append(key).append(" ");
                params.add(find.getObject(key).get("$lte"));
            } else if (find.getObject(key).containsKey("$in")) {
                // TODO:  Need to handle StringArray or IntegerArray
                sbWhere.append(key).append(" IN (").append(" ?").append(" ) ");
                params.add(find.getObject(key).get("$in"));
            } else if (find.getObject(key).containsKey("$like")) {
                sbWhere.append(key).append(" LIKE ").append(" ?").append(" ");
                params.add(find.getObject(key).get("$like"));
            } else if (find.getObject(key).containsKey("$reg")) {
                sbWhere.append(key).append(" REGEXP ").append(" ?").append(" ");
                params.add(find.getObject(key).get("$reg"));
            }
        } else {
            sbWhere.append(key).append("=").append(" ? ");
            params.add(find.get(key));
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
    public static ApiObject upsertWithIntegerKey(Connection conn, String tableName, String idField, ApiObject obj) throws ApiException {
        Integer idValue = null;

        if (obj.isSet(idField)) {
            idValue = obj.getInteger(idField);

            updateObject(conn, createObjectUpdateSql(tableName, idField, obj), obj);
        } else {  // Perform Insert
            idValue = insertObjectWithKey(conn, createObjectInsertSql(tableName, idField, obj), new String[]{idField}, obj);
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
    public static ApiObject upsertWithStringKey(Connection conn, String tableName, String idField, ApiObject obj) throws ApiException {
        String idValue = null;

        if (obj.isSet(idField)) {
            idValue = obj.getString(idField);

            updateObject(conn, createObjectUpdateSql(tableName, idField, obj), obj);
        } else {  // Perform Insert
            idValue = insertObjectWithStringKey(conn, createObjectInsertSql(tableName, idField, obj), new String[]{idField}, obj);
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

    /**
     * Upsert an Object to the database, given an id field.
     *
     * This does not return a generated Key Field.
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
    public static ApiObject upsertWithId(Connection conn, String tableName, String idField, ApiObject obj) throws ApiException {
        try {
            boolean performInsert = true;

            if (obj.containsKey(idField)) {
                Integer iCnt = SsDbUtils.queryForInt(conn,
                        String.format("SELECT COUNT(*) AS retCount FROM %s WHERE %s=?",
                                tableName, idField),
                        new Object[]{obj.get(idField)});

                if (iCnt > 0) {
                    performInsert = false;
                }
            }

            if (performInsert) {
                Tuple<String, Object[]> insertObj = createObjectInsertSqlDirect(tableName, idField, obj);
                SsDbUtils.update(conn, insertObj.getFirst(), insertObj.getSecond());
            } else {
                Tuple<String, Object[]> updateObj = createObjectUpdateSqlDirect(tableName, idField, obj);
                SsDbUtils.update(conn, updateObj.getFirst(), updateObj.getSecond());
            }

            ApiObject objRet = SsDbUtils.querySqlAsApiObject(conn,
                    String.format("SELECT * FROM %s WHERE %s=?",
                            tableName, idField),
                    new Object[]{obj.get(idField)});

            if (objRet.isSet("root")) {
                return objRet.getList("root").get(0);
            } else {
                throw new ApiException(500, "The Record was not created properly");
            }
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage());
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

    private static Tuple<String, Object[]> createObjectUpdateSqlDirect(String tableName, String idField, ApiObject obj) throws ApiException {
        if (obj.isEmpty()) {
            throw new ApiException("Object MUST not be empty");
        }

        if (!obj.containsKey(idField)) {
            throw new ApiException("Field: " + idField + " MUST be set");
        }

        List<Object> objFields = new ArrayList<>();

        StringBuilder sb = new StringBuilder();

        sb.append("UPDATE ");
        sb.append(tableName);
        sb.append(" SET ");

        boolean bFirst = true;

        for (String fldName : obj.keySet()) {
            if (!idField.equals(fldName)) {
                if (bFirst) {
                    bFirst = false;
                } else {
                    sb.append(",");
                }

                sb.append(fldName).append("=?");

                objFields.add(obj.get(fldName));
            }
        }

        sb.append(" WHERE ");
        sb.append(idField);
        sb.append("=?");

        objFields.add(obj.get(idField));

        Tuple<String, Object[]> retObj = new Tuple<>();

        retObj.setFirst(sb.toString());
        retObj.setSecond(objFields.toArray());

        return retObj;
    }

    private static Tuple<String, Object[]> createObjectInsertSqlDirect(String tableName, String idField, ApiObject obj) throws ApiException {
        if (obj.isEmpty()) {
            throw new ApiException("Object MUST not be empty");
        }

        List<Object> objFields = new ArrayList<>();

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
            sbFields.append("?");
            objFields.add(obj.get(fldName));
        }

        sb.append(") VALUES (");
        sb.append(sbFields);
        sb.append(")");

        Tuple<String, Object[]> retObj = new Tuple<>();
        retObj.setFirst(sb.toString());
        retObj.setSecond(objFields.toArray());

        return retObj;
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
