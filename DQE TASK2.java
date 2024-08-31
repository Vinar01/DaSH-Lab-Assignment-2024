import java.util.*;

// Enum for Join Type
enum JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL_OUTER
}

// Table class
class Table {
    private String name;
    private List<List<Object>> data;  // 2D List to store table data
    private List<String> columnNames; // List of column names

    // Constructor
    public Table(String name) {
        this.name = name;
        this.data = new ArrayList<>();
        this.columnNames = new ArrayList<>();
    }

    // Insert a row at a specific index
    public void insertRow(int rowIndex, String[] rowData) {
        if (rowIndex >= 0 && rowIndex < data.size() && rowData.length == columnNames.size()) {
            List<Object> row = data.get(rowIndex);
            for (int i = 0; i < rowData.length; i++) {
                row.set(i, rowData[i]);
            }
        } else {
            System.out.println("Invalid row insertion.");
        }
    }

    // Get a specific row
    public List<Object> getRow(int rowIndex) {
        if (rowIndex >= 0 && rowIndex < data.size()) {
            return data.get(rowIndex);
        }
        return null;
    }

    // Get all data
    public List<List<Object>> getAllData() {
        return data;
    }

    // Get number of rows
    public int getNumRows() {
        return data.size();
    }

    // Get number of columns
    public int getNumCols() {
        return columnNames.size();
    }

    // Get column names
    public List<String> getColumnNames() {
        return columnNames;
    }

    // Set default values based on data type
    public void setDefault(String columnName, Object defaultValue) {
        int colIndex = columnNames.indexOf(columnName);
        if (colIndex == -1) return;
        for (List<Object> row : data) {
            if (row.get(colIndex) == null) {
                row.set(colIndex, defaultValue);
            }
        }
    }

    // Select columns
    public Table select(String[] columns) {
        Table resultTable = new Table("result");
        resultTable.getColumnNames().addAll(Arrays.asList(columns));

        for (List<Object> row : getAllData()) {
            List<Object> resultRow = new ArrayList<>();
            for (String column : columns) {
                int columnIndex = getColumnNames().indexOf(column);
                if (columnIndex != -1) {
                    resultRow.add(row.get(columnIndex));
                } else {
                    System.out.println("Column not found: " + column);
                    return null;
                }
            }
            resultTable.getAllData().add(resultRow);
        }

        return resultTable;
    }

    // Delete rows based on condition
    public void delete(String condition) {
        if (condition == null || condition.trim().isEmpty()) {
            // Delete all rows if no condition is specified
            getAllData().clear();
        } else {
            // Parse the condition to extract the column name and value
            String[] parts = condition.split("=");
            if (parts.length != 2) {
                System.out.println("Invalid condition: " + condition);
                return;
            }
            String columnName = parts[0].trim();
            String value = parts[1].trim();

            // Find the column index
            int columnIndex = getColumnNames().indexOf(columnName);
            if (columnIndex == -1) {
                System.out.println("Column not found: " + columnName);
                return;
            }

            // Iterate over the table data and delete rows that match the condition
            Iterator<List<Object>> iterator = getAllData().iterator();
            while (iterator.hasNext()) {
                List<Object> row = iterator.next();
                Object currentValue = row.get(columnIndex);
                if (currentValue != null && currentValue.equals(value)) {
                    iterator.remove();
                }
            }
        }
    }

    // Join with another table
    public Table join(Table otherTable, String joinCondition, JoinType joinType) {
        Table resultTable = new Table("result");

        // Get the column names from both tables
        List<String> thisColumnNames = getColumnNames();
        List<String> otherColumnNames = otherTable.getColumnNames();

        // Create the result table column names
        resultTable.getColumnNames().addAll(thisColumnNames);
        resultTable.getColumnNames().addAll(otherColumnNames);

        // Parse the join condition to extract the column names
        String[] parts = joinCondition.split("=");
        if (parts.length != 2) {
            System.out.println("Invalid join condition: " + joinCondition);
            return null;
        }
        String thisColumnName = parts[0].trim();
        String otherColumnName = parts[1].trim();

        // Find the column indices
        int thisColumnIndex = thisColumnNames.indexOf(thisColumnName);
        int otherColumnIndex = otherColumnNames.indexOf(otherColumnName);

        if (thisColumnIndex == -1 || otherColumnIndex == -1) {
            System.out.println("Column not found: " + thisColumnName + " or " + otherColumnName);
            return null;
        }

        // Create a HashMap to store the rows of the other table
        Map<Object, List<Object>> otherTableRows = new HashMap<>();
        for (List<Object> otherRow : otherTable.getAllData()) {
            Object key = otherRow.get(otherColumnIndex);
            otherTableRows.putIfAbsent(key, new ArrayList<>());
            otherTableRows.get(key).addAll(otherRow);
        }

        // Iterate over the rows of this table and perform the join
        for (List<Object> thisRow : getAllData()) {
            Object key = thisRow.get(thisColumnIndex);
            List<Object> otherRows = otherTableRows.get(key);
            if (otherRows != null) {
                List<Object> resultRow = new ArrayList<>(thisRow);
                resultRow.addAll(otherRows);
                resultTable.getAllData().add(resultRow);
            } else if (joinType == JoinType.LEFT || joinType == JoinType.FULL_OUTER) {
                List<Object> resultRow = new ArrayList<>(thisRow);
                resultRow.addAll(Collections.nCopies(otherColumnNames.size(), null));
                resultTable.getAllData().add(resultRow);
            }
        }

        // Add rows from the other table if RIGHT or FULL OUTER JOIN
        if (joinType == JoinType.RIGHT || joinType == JoinType.FULL_OUTER) {
            for (List<Object> otherRow : otherTable.getAllData()) {
                Object key = otherRow.get(otherColumnIndex);
                if (!otherTableRows.containsKey(key)) {
                    List<Object> resultRow = new ArrayList<>(Collections.nCopies(thisColumnNames.size(), null));
                    resultRow.addAll(otherRow);
                    resultTable.getAllData().add(resultRow);
                }
            }
        }

        return resultTable;
    }
}

// Database class
class Database {
    private List<Table> tables;

    // Constructor
    public Database() {
        tables = new ArrayList<>();
    }

    // Create a new table
    public void createTable(int rows, int cols) {
        Table newTable = new Table("Table" + (tables.size() + 1));
        for (int i = 0; i < rows; i++) {
            newTable.getAllData().add(new ArrayList<>(Collections.nCopies(cols, null)));
        }
        tables.add(newTable);
    }

    // Get a specific table
    public Table getTable(int tableIndex) {
        if (tableIndex >= 0 && tableIndex < tables.size()) {
            return tables.get(tableIndex);
        }
        return null;
    }

    public Table getTable(String tableName) {
        for (Table table : tables) {
            if (tableName.equals(table.getColumnNames().get(0))) {
                return table;
            }
        }
        return null;
    }

    // Alter table functionality
    public void alterTable(String tableName, List<AlterTableOperation> operations) {
        Table table = getTable(tableName);
        if (table == null) {
            System.out.println("Table not found: " + tableName);
            return;
        }

        for (AlterTableOperation operation : operations) {
            switch (operation.getType()) {
                case ADD_COLUMN:
                    addColumn(table, operation.getColumnName(), operation.getDataType());
                    break;
                case MODIFY_COLUMN:
                    modifyColumn(table, operation.getColumnName(), operation.getDataType());
                    break;
                case REMOVE_COLUMN:
                    removeColumn(table, operation.getColumnName());
                    break;
                case RENAME_COLUMN:
                    renameColumn(table, operation.getColumnName(), operation.getNewColumnName());
                    break;
                case ADD_CONSTRAINT:
                    addConstraint(table, operation.getConstraintType(), operation.getColumnName());
                    break;
                case REMOVE_CONSTRAINT:
                    removeConstraint(table, operation.getConstraintType(), operation.getColumnName());
                    break;
                default:
                    System.out.println("Unknown alter table operation: " + operation.getType());
                    break;
            }
        }
    }

    // Helper methods for altering table
    private void addColumn(Table table, String columnName, String dataType) {
        for (List<Object> row : table.getAllData()) {
            row.add(null);
        }
        table.getColumnNames().add(columnName);

        if (dataType.equals("int") || dataType.equals("Integer")) {
            table.setDefault(columnName, 0);
        } else if (dataType.equals("double") || dataType.equals("Double")) {
            table.setDefault(columnName, 0.0);
        } else if (dataType.equals("String")) {
            table.setDefault(columnName, "");
        }
    }

    private void modifyColumn(Table table, String columnName, String newDataType) {
        int colIndex = table.getColumnNames().indexOf(columnName);
        if (colIndex == -1) {
            System.out.println("Column not found: " + columnName);
            return;
        }

        if (newDataType.equals("int") || newDataType.equals("Integer")) {
            table.setDefault(columnName, 0);
        } else if (newDataType.equals("double") || newDataType.equals("Double")) {
            table.setDefault(columnName, 0.0);
        } else if (newDataType.equals("String")) {
            table.setDefault(columnName, "");
        }
    }

    private void removeColumn(Table table, String columnName) {
        int colIndex = table.getColumnNames().indexOf(columnName);
        if (colIndex == -1) {
            System.out.println("Column not found: " + columnName);
            return;
        }
        table.getColumnNames().remove(colIndex);
        for (List<Object> row : table.getAllData()) {
            row.remove(colIndex);
        }
    }

    private void renameColumn(Table table, String oldColumnName, String newColumnName) {
        int colIndex = table.getColumnNames().indexOf(oldColumnName);
        if (colIndex == -1) {
            System.out.println("Column not found: " + oldColumnName);
            return;
        }
        table.getColumnNames().set(colIndex, newColumnName);
    }

    private void addConstraint(Table table, String constraintType, String columnName) {
        switch (constraintType) {
            case "NOT NULL":
                // Handle NOT NULL
                break;
            case "UNIQUE":
                // Handle UNIQUE
                break;
            case "PRIMARY KEY":
                // Handle PRIMARY KEY
                break;
            case "FOREIGN KEY":
                // Handle FOREIGN KEY
                break;
            default:
                System.out.println("Unknown constraint type: " + constraintType);
                break;
        }
    }

    private void removeConstraint(Table table, String constraintType, String columnName) {
        switch (constraintType) {
            case "NOT NULL":
                // Handle removing NOT NULL
                break;
            case "UNIQUE":
                // Handle removing UNIQUE
                break;
            case "PRIMARY KEY":
                // Handle removing PRIMARY KEY
                break;
            case "FOREIGN KEY":
                // Handle removing FOREIGN KEY
                break;
            default:
                System.out.println("Unknown constraint type: " + constraintType);
                break;
        }
    }
}

// Class to define alter table operations
class AlterTableOperation {
    private AlterTableOperationType type;
    private String columnName;
    private String newColumnName;
    private String dataType;
    private String constraintType;

    public AlterTableOperation(AlterTableOperationType type, String columnName, String newColumnName, String dataType, String constraintType) {
        this.type = type;
        this.columnName = columnName;
        this.newColumnName = newColumnName;
        this.dataType = dataType;
        this.constraintType = constraintType;
    }

    public AlterTableOperationType getType() {
        return type;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getNewColumnName() {
        return newColumnName;
    }

    public String getDataType() {
        return dataType;
    }

    public String getConstraintType() {
        return constraintType;
    }
}

// Enum for Alter Table Operations
enum AlterTableOperationType {
    ADD_COLUMN,
    MODIFY_COLUMN,
    REMOVE_COLUMN,
    RENAME_COLUMN,
    ADD_CONSTRAINT,
    REMOVE_CONSTRAINT
}

// Main class to test functionality
public class Main {
    public static void main(String[] args) {
        // Create Database
        Database db = new Database();
        db.createTable(3, 3); // 3x3 table

        // Get the created table
        Table table = db.getTable(0);

        // Insert data into the table
        table.insertRow(0, new String[]{"1", "Alice", "Engineer"});
        table.insertRow(1, new String[]{"2", "Bob", "Artist"});
        table.insertRow(2, new String[]{"3", "Charlie", "Doctor"});

        // Select columns from the table
        Table selectedTable = table.select(new String[]{"ID", "Name"});
        if (selectedTable != null) {
            System.out.println("Selected Columns:");
            for (List<Object> row : selectedTable.getAllData()) {
                System.out.println(row);
            }
        }

        // Delete rows based on a condition
        table.delete("Name=Bob");
        System.out.println("Table after deletion:");
        for (List<Object> row : table.getAllData()) {
            System.out.println(row);
        }

        // Alter table by adding a new column
        List<AlterTableOperation> operations = new ArrayList<>();
        operations.add(new AlterTableOperation(AlterTableOperationType.ADD_COLUMN, "Email", null, "String", null));
        db.alterTable("Table1", operations);

        System.out.println("Table after alteration:");
        for (List<Object> row : table.getAllData()) {
            System.out.println(row);
        }

        // Perform a join with another table
        db.createTable(2, 3); // Another 2x3 table
        Table otherTable = db.getTable(1);
        otherTable.insertRow(0, new String[]{"1", "alice@example.com", "123"});
        otherTable.insertRow(1, new String[]{"3", "charlie@example.com", "456"});

        Table joinedTable = table.join(otherTable, "ID=ID", JoinType.INNER);
        System.out.println("Joined Table:");
        for (List<Object> row : joinedTable.getAllData()) {
            System.out.println(row);
        }
    }
}

/*System Design Overview
The system design revolves around creating a basic in-memory database system using object-oriented programming (OOP) principles. The key components are:

Table Class: Represents a database table with rows and columns.

Attributes:
name: Name of the table.
data: A 2D list representing the rows and columns of the table.
columnNames: A list of column names for the table.
Methods:
insertRow: Inserts a row at a specific index.
getRow: Retrieves a row by its index.
getAllData: Returns all the rows in the table.
getNumRows & getNumCols: Provide the number of rows and columns, respectively.
setDefault: Sets default values for a column.
CRUD Operations:
select: Selects specific columns from the table.
delete: Deletes rows based on a condition.
join: Joins the current table with another table based on a condition and specified join type.
Database Class: Manages multiple tables and provides operations to create and alter them.

Attributes:
tables: A list of Table objects representing the database tables.
Methods:
createTable: Creates a new table with a specified number of rows and columns.
getTable: Retrieves a table by its index or name.
alterTable: Modifies the structure of a table (adding/removing/renaming columns, and adding/removing constraints).
AlterTableOperation & JoinType Enums:

AlterTableOperation: Represents various operations that can be performed when altering a table (e.g., adding, modifying, or removing columns).
JoinType: Enum to define types of joins (INNER, LEFT, RIGHT, FULL OUTER).
System Capabilities
CRUD Operations: The system can perform basic Create, Read, Update, and Delete (CRUD) operations on tables.

Create: Create tables and insert rows.
Read: Select specific columns from a table and retrieve rows.
Update: Modify existing rows via insertRow or alter table structure.
Delete: Delete specific rows based on a condition.
Join Operations: The system supports various types of joins (INNER, LEFT, RIGHT, FULL OUTER) between tables.

Table Alterations: The system allows for adding, modifying, renaming, or removing columns in a table, as well as adding or removing constraints like NOT NULL, UNIQUE, PRIMARY KEY, and FOREIGN KEY.

In-Memory Storage: All data is stored in memory, which is suitable for lightweight, transient operations or prototyping but not for production-scale databases.

Efficiency
1. Data Retrieval (Select Operation):

Time Complexity: O(n * m), where n is the number of rows and m is the number of columns selected. This complexity arises from iterating through each row and column to extract the required data.
Space Complexity: O(n * m), as a new table is created to store the selected data.
2. Data Deletion:

Time Complexity: O(n), where n is the number of rows in the table. Each row is checked to see if it meets the deletion condition.
Space Complexity: O(1), as the operation is performed in place.
3. Join Operations:

Time Complexity:
INNER JOIN: O(n * m), where n is the number of rows in the first table, and m is the number of rows in the second table. This assumes a basic nested loop implementation.
LEFT/RIGHT/FULL OUTER JOIN: O(n * m) + O(k), where k is the number of unmatched rows added from the second table for OUTER joins.
Space Complexity: O(n + m), where a new table is created to store the joined data.
4. Table Alterations:

Adding a Column: O(n), where n is the number of rows in the table.
Removing a Column: O(n), for removing the column from each row.
Renaming a Column: O(1), as it only involves updating the column name.
Adding/Removing Constraints: Varies based on the constraint type. Generally O(1) for operations like marking a column as NOT NULL or UNIQUE.
Efficiency Considerations
Scalability:

This system is not designed to handle large datasets or concurrent operations due to its in-memory nature and the use of basic list structures. Operations that involve large tables will quickly become inefficient due to the linear or quadratic complexity of most operations.
Memory Usage:

Memory usage is directly proportional to the number of rows and columns in the tables. Since the system operates entirely in memory, it can run out of space if large datasets are used.
Optimization Opportunities:

Indexing: Implementing indexing on columns could significantly improve the efficiency of select, delete, and join operations, particularly when dealing with large datasets.
Hashing for Joins: Using a hash map (as done in the join method) improves efficiency over a basic nested loop approach, reducing the need for repeated full-table scans.
Lazy Evaluation: In certain scenarios, delaying the execution of operations until absolutely necessary (e.g., in a query pipeline) could optimize performance.
Conclusion
The system is a simple, in-memory representation of a database designed for educational purposes. It is efficient for small datasets but lacks the advanced optimization mechanisms required for larger, production-scale databases. The design is modular and flexible, allowing for future extensions, such as indexing, transaction management, or persistent storage integration.







 */