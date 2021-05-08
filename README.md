# SsDbUtils
Super Simple DB Utilities

# Purpose

This is a set of tools I have used for several years to help with JDBC connections.

# Example Usage

```java
try (Connection conn = _db.getConnection()) {
    SsDbUtils.update(conn, "INSERT INTO table (name, test) VALUES (?, ?)", 
        new Object[] { "myName", "myTest" });
} catch (SQLException sqx) {
    log.error("SQL Exception", sqx);
}
```

# ApiObject Examples

```java
ApiObject obj = new ApiObject();

obj.setString("name", "myName");
obj.setString("test", "myTest");

try (Connection conn = _db.getConnection()) {
    SsDbUtils.updateObject(conn, "INSERT INTO table (name, test) VALUES (:name, :test)", obj);
} catch (SQLException sqx) {
    log.error("SQL Exception", sqx);
}
```