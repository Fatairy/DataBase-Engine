import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.SynchronousQueue;

public class DBApp {
	public Vector<Table> tables;

	public static void main(String[] args) throws IOException, DBAppException {
		DBApp app = new DBApp();
		
		/*Hashtable<String, String> cols = new Hashtable<>();
		cols.put("num", "java.lang.Integer");
		cols.put("type", "java.lang.String");
		app.createTable("numbers", "num", cols);
		
		for (int i = 0; i < 100; i++) {
			Hashtable<String, Object> row = new Hashtable<>();
			row.put("num", i);
			row.put("type", i % 2 == 0 ? "even" : "odd");
			app.insertIntoTable("numbers", row);
		}

		app.createBitmapIndex("numbers", "num");
		app.createBitmapIndex("numbers", "type");*/
		
		/*Iterator iter = app.selectFromTable(
				new SQLTerm[] { new SQLTerm("numbers", "type", "=", "odd"), new SQLTerm("numbers", "num", ">", 50) },
				new String[] { "AND" });
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}*/
		
		for (Table table : app.tables) {
			System.out.println("Table name: " + table.name);
			int pageI = 0;
			for (Page page : table.pages) {
				page.load(table.name + "_" + pageI++ + ".class");
				for (Hashtable<String, Object> pageRow : page.data) {
					System.out.println(pageRow);
				}
				page.unload();
			}
			System.out.println();
			System.out.println("Indices:");
			for (Entry<String, BitmapIndex> index : table.colNameIndex.entrySet()) {
				System.out.println("*" + index.getKey());
				for (IndexPage page : index.getValue().pages) {
					for (Tuple<Comparable, BitSet> indexRow : page.bitmap) {
						System.out.println(indexRow.x + " " + indexRow.y);
					}
				}
			}
			System.out.println("---------------");
		}
	}

	public DBApp() throws IOException {
		tables = new Vector<>();
		
		try {
			FileReader fileReader = new FileReader("metadata.csv");
			BufferedReader reader = new BufferedReader(fileReader);
			String file = "";
			String line;
			while ((line = reader.readLine()) != null) {
				file += line + "\n";
			}
			fromMetadata(file);
			fileReader.close();
		} catch (FileNotFoundException e) { }
	}

	public void createTable(String tableName, String clusteringKeyColumn, Hashtable<String, String> colNameType) throws DBAppException {
		Table table = getTable(tableName);
		if (table != null) {
			throw new DBAppException("Table already exists");
		}
		tables.add(new Table(tableName, clusteringKeyColumn, colNameType));
		
		try {
			String file = getMetadata();
			FileWriter fileWriter = new FileWriter("metadata.csv");
			fileWriter.write(file);
			fileWriter.close();
		} catch (IOException e) {
			System.out.println("Failed to write metadata file");
		}
	}

	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		Table table = getTable(tableName);
		if (table == null) {
			throw new DBAppException("Table does not exist");
		}
		table.insert(colNameValue);
	}

	public void updateTable(String tableName, String key, Hashtable<String, Object> colNameValue)
			throws DBAppException {
		Table table = getTable(tableName);
		if (table == null) {
			throw new DBAppException("Table does not exist");
		}
		table.update(key, colNameValue);

		// column attributes compatible
	}

	public void deleteFromTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		Table table = getTable(tableName);
		if (table == null) {
			throw new DBAppException("Table does not exist");
		}
		table.delete(colNameValue);
	}
	
	public void createBitmapIndex(String tableName, String colName) throws DBAppException {
		Table table = getTable(tableName);
		if (table == null) {
			throw new DBAppException("Table does not exist");
		}
		table.createIndex(colName);
		
		try {
			String file = getMetadata();
			FileWriter fileWriter = new FileWriter("metadata.csv");
			fileWriter.write(file);
			fileWriter.close();
		} catch (IOException e) {
			System.out.println("Failed to write metadata file");
		}
	}

	public Table getTable(String tableName) {
		for (Table table : tables) {
			if (table.name.equals(tableName)) {
				return table;
			}
		}

		return null;
	}
	
	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] operators) throws DBAppException {
		ArrayList<BitSet> results = new ArrayList<>();
		Table table = null;
		
		for (SQLTerm sqlTerm : sqlTerms) {
			table = getTable(sqlTerm._strTableName);
			if (table == null) {
				throw new DBAppException("Table " + sqlTerm._strColumnName + " does not exist");
			}
			if (!table.colNameType.containsKey(sqlTerm._strColumnName)) {
				throw new DBAppException("Column " + sqlTerm._strTableName + "." + sqlTerm._strColumnName + " does not exist");
			}
			
			results.add(table.select(sqlTerm));
		}
		
		BitSet aggregate = new BitSet();
		if (sqlTerms.length >= 1) {
			aggregate.or(results.get(0));
			
			for (int i = 0; i < operators.length; i++) {
				if (operators[i].equals("AND")) {
					aggregate.and(results.get(i + 1));
				} else if (operators[i].equals("OR")) {
					aggregate.or(results.get(i + 1));
				} else if (operators[i].equals("XOR")) {
					aggregate.xor(results.get(i + 1));
				}
			}
		}
		
		return new ResultSet(table, aggregate);
	}

	public String getMetadata() {
		String metadata = "Table Name,Column Name,Column Type,Key,Indexed\n";

		for (Table table : tables) {
			for (Map.Entry<String, String> col : table.colNameType.entrySet()) {
				metadata += String.format("%s,%s,%s,%s,%s\n", table.name, col.getKey(), col.getValue(),
						col.getKey().equals(table.clusteringKeyColumn), table.colNameIndex.containsKey(col.getKey()));
			}
		}

		return metadata;
	}

	public void fromMetadata(String metadata) {
		StringTokenizer token = new StringTokenizer(metadata, ",\r\n");

		token.nextToken("\r\n");

		while (token.hasMoreTokens()) {
			String tableName = token.nextToken(",\r\n");
			String colName = token.nextToken();
			String colType = token.nextToken();
			boolean isKey = Boolean.parseBoolean(token.nextToken());
			boolean isIndexed = Boolean.parseBoolean(token.nextToken());

			Table table = getTable(tableName);
			if (table == null) {
				table = new Table(tableName, "", new Hashtable<>());
				tables.add(table);
			}

			table.colNameType.put(colName, colType);
			if (isKey) {
				table.clusteringKeyColumn = colName;
			}
			if (isIndexed) {
				try {
					table.loadIndex(colName);
				} catch (IOException e) {
					System.out.println("Failed to load index for column " + tableName + "." + colName);
				}
			}
		}
	}
}
