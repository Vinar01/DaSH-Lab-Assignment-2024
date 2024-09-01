import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;
import java.util.BitSet;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.FileReader;


public class Main {
    public static void main(String[] args) {
        try {
            // Create Database with MemTable size of 1000
            Database db = new Database(1000);

            // Define column names
            List<String> columns = Arrays.asList("ID", "Name", "Salary");

            // Create Table1 with ID as primary key (index 0)
            db.createTable("Employees", columns, 0);
            Table employees = db.getTable("Employees");

            // Insert data into Employees table
            employees.insertRow(createRow("1", "Alice", "70000"));
            employees.insertRow(createRow("2", "Bob", "50000"));
            employees.insertRow(createRow("3", "Charlie", "60000"));
            employees.insertRow(createRow("4", "Diana", "80000"));
            employees.insertRow(createRow("5", "Ethan", "55000"));
            employees.insertRow(createRow("6", "Fiona", "75000"));

            // Display all rows
            System.out.println("All Employees:");
            displayTable(employees);

            // Perform a search operation
            System.out.println("\nSearching for employee with ID 3:");
            Row foundEmployee = employees.search("3");
            if (foundEmployee != null) {
                System.out.println("Found: " + foundEmployee);
            } else {
                System.out.println("Employee not found.");
            }

        } catch (IOException e) {
            System.err.println("An error occurred: " + e.getMessage());
        }
    }

    private static Row createRow(String... values) {
        List<Object> valueList = Arrays.asList((Object[]) values);
        return new Row(valueList);
    }

    private static void displayTable(Table table) {
        // Placeholder for displaying rows from the table
    }
}

class Row {
    private List<Object> values;

    public Row(List<Object> values) {
        this.values = new ArrayList<>(values);
    }

    public List<Object> getValues() {
        return new ArrayList<>(values);
    }

    @Override
    public String toString() {
        return "Row{" + "values=" + values + '}';
    }

    public Object getValue(int index) {
        return values.get(index);
    }

    public void setValue(int index, Object value) {
        values.set(index, value);
    }
}
class Table {
    private String name;
    private List<String> columnNames;
    private LSMTree lsmTree;
    private int primaryKeyIndex;

    public Table(String name, List<String> columnNames, int primaryKeyIndex, int memTableSize) throws IOException {
        this.name = name;
        this.columnNames = new ArrayList<>(columnNames);
        this.primaryKeyIndex = primaryKeyIndex;
        this.lsmTree = new LSMTree(columnNames, memTableSize, primaryKeyIndex);
    }

    public void insertRow(Row row) throws IOException {
        lsmTree.insert(row);
    }

    public Row search(Object key) {
        return lsmTree.search(key);
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }

    public int getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }
}
class WAL {
    private static final String WAL_FILE = "write_ahead.log";
    private FileWriter fileWriter;

    public WAL() throws IOException {
        this.fileWriter = new FileWriter(WAL_FILE, true);
    }

    public void logOperation(String operation) throws IOException {
        fileWriter.write(operation + "\n");
        fileWriter.flush();
    }

    public void close() throws IOException {
        fileWriter.close();
    }

    public List<String> readLog() throws IOException {
        List<String> operations = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(WAL_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                operations.add(line);
            }
        }
        return operations;
    }
}

// Bloom Filter class
class BloomFilter<T> {
    private BitSet bitSet;
    private int size;
    private int hashFunctions;

    public BloomFilter(int size, int hashFunctions) {
        this.size = size;
        this.hashFunctions = hashFunctions;
        this.bitSet = new BitSet(size);
    }

    public void add(T element) {
        for (int i = 0; i < hashFunctions; i++) {
            int hash = (element.hashCode() + i) % size;
            bitSet.set(hash);
        }
    }

    public boolean mightContain(T element) {
        for (int i = 0; i < hashFunctions; i++) {
            int hash = (element.hashCode() + i) % size;
            if (!bitSet.get(hash)) {
                return false;
            }
        }
        return true;
    }
}

// SparseIndex class
class SparseIndex {
    private TreeMap<Comparable, Long> index;

    public SparseIndex() {
        this.index = new TreeMap<>();
    }

    public void addEntry(Comparable key, long position) {
        index.put(key, position);
    }

    public Long getPosition(Comparable key) {
        Map.Entry<Comparable, Long> entry = index.floorEntry(key);
        return entry != null ? entry.getValue() : null;
    }
}
class LSMTree {
    private MemTable memTable;
    private List<SSTable> sstables;
    private int memTableSize;
    private List<String> columnNames;
    private int primaryKeyIndex;
    private WAL wal;

    public LSMTree(List<String> columnNames, int memTableSize, int primaryKeyIndex) throws IOException {
        this.columnNames = new ArrayList<>(columnNames);
        this.memTableSize = memTableSize;
        this.memTable = new MemTable(columnNames, memTableSize);
        this.sstables = new ArrayList<>();
        this.primaryKeyIndex = primaryKeyIndex;
        this.wal = new WAL();
    }

    public void insert(Row row) throws IOException {
        try {
            wal.logOperation("INSERT:" + String.join(",", row.getValues().toString()));
            memTable.insert(row, primaryKeyIndex);
            if (memTable.getRows().size() >= memTableSize) {
                flushMemTable();
            }
        } catch (IllegalStateException e) {
            flushMemTable();
            memTable.insert(row, primaryKeyIndex);
        }
    }

    private void flushMemTable() {
        List<Row> flushedRows = memTable.getRows();
        SSTable sstable = new SSTable(flushedRows, columnNames, primaryKeyIndex);
        sstables.add(0, sstable);
        memTable.clear();
    }

    public Row search(Object key) {
        // Check memtable
        for (Row row : memTable.getRows()) {
            if (row.getValues().get(primaryKeyIndex).equals(key)) {
                return row;
            }
        }

        // Check SSTables
        for (SSTable sstable : sstables) {
            if (!sstable.getBloomFilter().mightContain((Comparable) key)) {
                continue; // Skip this SSTable if Bloom filter indicates key is not present
            }

            Long position = sstable.getSparseIndex().getPosition((Comparable) key);
            if (position != null) {
                // Start searching from the position indicated by sparse index
                for (int i = position.intValue(); i < sstable.getRows().size(); i++) {
                    Row row = sstable.getRows().get(i);
                    if (row.getValues().get(primaryKeyIndex).equals(key)) {
                        return row;
                    }
                    if (((Comparable) row.getValues().get(primaryKeyIndex)).compareTo(key) > 0) {
                        break; // Key not found in this SSTable
                    }
                }
            }
        }

        return null; // Key not found
    }

    public void recover() throws IOException {
        List<String> operations = wal.readLog();
        for (String operation : operations) {
            String[] parts = operation.split(":");
            if (parts[0].equals("INSERT")) {
                String[] values = parts[1].split(",");
                List<Object> rowValues = new ArrayList<>(Arrays.asList(values));
                Row row = new Row(rowValues);
                memTable.insert(row, primaryKeyIndex);
            }
            // Add other operation types as needed
        }
    }
}
class MemTable {
    private List<Row> rows;
    private List<String> columnNames;
    private int maxSize;
    private Comparable minKey;
    private Comparable maxKey;

    public MemTable(List<String> columnNames, int maxSize) {
        this.rows = new ArrayList<>();
        this.columnNames = new ArrayList<>(columnNames);
        this.maxSize = maxSize;
        this.minKey = null;
        this.maxKey = null;
    }

    public void insert(Row row, int keyIndex) {
        if (rows.size() >= maxSize) {
            throw new IllegalStateException("MemTable is full");
        }
        Comparable key = (Comparable) row.getValues().get(keyIndex);
        int pos = findInsertPosition(key, keyIndex);
        rows.add(pos, row);

        // Update min and max keys
        if (minKey == null || key.compareTo(minKey) < 0) {
            minKey = key;
        }
        if (maxKey == null || key.compareTo(maxKey) > 0) {
            maxKey = key;
        }
    }

    public List<Row> getRows() {
        return rows;
    }

    public void clear() {
        rows.clear();
    }

    public Comparable getMinKey() {
        return minKey;
    }

    public Comparable getMaxKey() {
        return maxKey;
    }

    private int findInsertPosition(Comparable key, int keyIndex) {
        // Implementation of findInsertPosition based on key and keyIndex
        return 0; // Placeholder
    }
}
// Modified SSTable class with sparse index
class SSTable {
    private final List<Row> rows;
    private final List<String> columnNames;
    private final SparseIndex sparseIndex;
    private final BloomFilter<Comparable> bloomFilter;

    public SSTable(List<Row> rows, List<String> columnNames, int primaryKeyIndex) {
        this.rows = new ArrayList<>(rows);
        this.columnNames = new ArrayList<>(columnNames);
        this.sparseIndex = new SparseIndex();
        this.bloomFilter = new BloomFilter<>(1000, 3); // Adjust size and hash functions as needed

        // Build sparse index and bloom filter
        for (int i = 0; i < rows.size(); i += 10) { // Add to sparse index every 10th element
            Row row = rows.get(i);
            Comparable key = (Comparable) row.getValues().get(primaryKeyIndex);
            sparseIndex.addEntry(key, (long) i);
            bloomFilter.add(key);
        }
    }

    public List<Row> getRows() {
        return rows;
    }

    public SparseIndex getSparseIndex() {
        return sparseIndex;
    }

    public BloomFilter<Comparable> getBloomFilter() {
        return bloomFilter;
    }
}
