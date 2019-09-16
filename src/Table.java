import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class Table {
	public String name;
	public String clusteringKeyColumn;
	public Hashtable<String, String> colNameType;
	public Hashtable<String, BitmapIndex> colNameIndex;
	
	public Vector<Page> pages;

	public Table(String name, String clusteringKeyColumn, Hashtable<String, String> colNameType) {
		this.name = name;
		this.clusteringKeyColumn = clusteringKeyColumn;
		this.colNameType = new Hashtable<>();
		for (String colName : colNameType.keySet()) {
			this.colNameType.put(colName, colNameType.get(colName));
		}
		this.colNameIndex = new Hashtable<>();

		this.pages = new Vector<>();
		
		File dir = new File(".");
		File[] files = dir.listFiles();
		Page[] foundPages = new Page[files.length];
		for (File file : files) {
			if (file.getName().startsWith(name + "_") && file.getName().endsWith(".class")) {
				String fileName = file.getName();
				fileName = fileName.substring((name + "_").length());
				fileName = fileName.substring(0, fileName.length() - 6);
				int pageI = Integer.parseInt(fileName);
				
				foundPages[pageI] = new Page();
				//foundPages[pageI].load(file.getName());
			}
		}
		
		for (Page page : foundPages) {
			if (page != null) {
				pages.add(page);
			}
		}
	}
	
	public void createIndex(String colName) throws DBAppException {
		BitmapIndex index = colNameIndex.get(colName);
		if (index != null) {
			throw new DBAppException("Index already exists");
		}
		index = new BitmapIndex();
		colNameIndex.put(colName, index);
		
		int rowI = 0;
		for (int pageI = 0; pageI < pages.size(); pageI++) {
			Page page = pages.get(pageI);
			try {
				page.load(name + "_" + pageI + ".class");
			} catch (IOException e) {
				throw new DBAppException("IO error");
			}
			
			for (Hashtable<String, Object> row : page.data) {
				index.insert((Comparable)row.get(colName), rowI);
				rowI++;
			}
			
			page.unload();
		}
		try {
			index.save(name, colName);
		} catch (IOException e) {
			System.out.println("Failed to save index");
		}
	}
	
	public void loadIndex(String colName) throws IOException {
		BitmapIndex index = colNameIndex.get(colName);
		if (index == null) {
			index = new BitmapIndex();
			colNameIndex.put(colName, index);
		}
		index.load(name, colName);
	}
	
	public void saveIndex(String colName) throws IOException, DBAppException {
		BitmapIndex index = colNameIndex.get(colName);
		if (index == null) {
			throw new DBAppException("Column is not indexed");
		}
		index.save(name, colName);
	}
	
	public void unloadPages() {
		for (Page page : pages) {
			page.unload();
		}
	}

	public void writeAndUnloadPages() {
		//delete non-existing pages
		File dir = new File(".");
		File[] files = dir.listFiles();
		Page[] loadedPages = new Page[files.length];
		for (File file : files) {
			if (file.getName().startsWith(name + "_") && file.getName().endsWith(".class")) {
				String fileName = file.getName();
				fileName = fileName.substring((name + "_").length());
				fileName = fileName.substring(0, fileName.length() - 6);
				int pageI = Integer.parseInt(fileName);
				
				if (pageI >= pages.size()) {
					file.delete();
				}
			}
		}
		
		for (int pageI = 0; pageI < pages.size(); pageI++) {
			try {
				Page page = pages.get(pageI);
				if (page.loaded) {
					page.save(name + "_" + pageI + ".class");
					page.unload();
				}
			} catch (IOException e) {
				System.out.println("Failed to write page #" + pageI + " for table " + name);
			}
		}
	}
	
	public void insert(Hashtable<String, Object> colNameValue) throws DBAppException {
		if (!checkRowCompatibility(colNameValue)) {
			throw new DBAppException("Invalid column values");
		}
		
		Tuple<Integer, Integer> insertPos = findInsertPosition(clusteringKeyColumn,
				(Comparable) colNameValue.get(clusteringKeyColumn));

		int pageI = insertPos.x;
		int rowI = insertPos.y;
		
		if (pageI < pages.size()) {
			try {
				pages.get(pageI).load(name + "_" + pageI + ".class");
			} catch (IOException e) {
				throw new DBAppException("IO error");
			}
		}
		
		if (pageI < pages.size() && rowI < pages.get(pageI).data.size()) {
			if (pages.get(pageI).data.get(rowI).get(clusteringKeyColumn).equals(colNameValue.get(clusteringKeyColumn))) {
				throw new DBAppException("Duplicate value for clustering key");
			}
		}
		
		Hashtable<String, Object> row = new Hashtable<>();
		for (String colName : colNameValue.keySet()) {
			row.put(colName, colNameValue.get(colName));
		}
		row.put("TouchDate", new Date());
		
		if (pageI >= pages.size()) {
			Page page = new Page();
			page.init();
			pages.add(page);
			page.data.add(row);
		} else {
			pages.get(pageI).data.add(rowI, row);
			if (pages.get(pageI).data.size() > Page.pageMaximum) {
				Hashtable<String, Object> lastRow = pages.get(pageI).data.remove(Page.pageMaximum);
				Page newPage = new Page();
				newPage.init();
				newPage.data.add(lastRow);
				pages.add(pageI+1, newPage);
				shiftPageIndicesAt(pageI+1, true);
			}
		}
		
		writeAndUnloadPages();
		
		int absPos = getAbsoluteRowPosition(pageI, rowI);
		for (String colName : colNameValue.keySet()) {
			BitmapIndex index = colNameIndex.get(colName);
			if (index != null) {
				index.insert((Comparable)colNameValue.get(colName), absPos);
				try {
					index.save(name, colName);
				} catch (IOException e) {
					System.out.println("Failed to save index for column " + colName);
				}
			}
		}
	}
	
	public void shiftPageIndicesAt(int index, boolean shiftUp) {
		Vector<String> moved = new Vector<>();
		
		File dir = new File(".");
		File[] files = dir.listFiles();
		Page[] loadedPages = new Page[files.length];
		for (File file : files) {
			if (file.getName().startsWith(name + "_") && file.getName().endsWith(".class")) {
				String fileName = file.getName();
				fileName = fileName.substring((name + "_").length());
				fileName = fileName.substring(0, fileName.length() - 6);
				int pageI = Integer.parseInt(fileName);
				
				if (pageI >= index) {
					if (shiftUp) {
						pageI++;
					} else {
						pageI--;
					}
					String newName = "__" + name + "_" + pageI + ".class";
					file.renameTo(new File(newName));
					moved.add(newName);
				}
			}
		}
		
		for (String fileName : moved) {
			//remove existing file first
			File old = new File(fileName.substring(2));
			if (old.exists()) {
				old.delete();
			}
			
			File file = new File(fileName);
			file.renameTo(new File(fileName.substring(2)));
		}
	}

	public void update(String key, Hashtable<String, Object> colNameValue) throws DBAppException {
		if (!checkPartialRowCompatibility(colNameValue)) {
			throw new DBAppException("Invalid column values");
		}
		
		boolean found = false;
		int foundPage = 0;
		ArrayList<Integer> foundRows;
		
		for (int pageI = 0; pageI < pages.size(); pageI++) {
			Page page = pages.get(pageI);
			try {
				page.load(name + "_" + pageI + ".class");
			} catch (IOException e) {
				throw new DBAppException("IO error");
			}
			
			boolean foundHere = false;
			foundRows = new ArrayList<>();
			for (Hashtable<String, Object> row : page.data) {
				//if (row.get(key).equals(colNameValue.get(key))) {
				if (row.get(clusteringKeyColumn).toString().equals(key)) {
					found = true;
					foundHere = true;
					foundPage = pageI;
					foundRows.add(page.data.indexOf(row));
					for (String colName : colNameValue.keySet()) {
						if (!colName.equals(clusteringKeyColumn)) {
							row.put(colName, colNameValue.get(colName));
						}
					}
					row.put("TouchDate", new Date());
				}
			}
			
			if (foundHere) {
				try {
					page.save(name + "_" + pageI + ".class");
				} catch (IOException e) {
					throw new DBAppException("IO error");
				}
			}
			page.unload();
			
			if (foundHere) {
				for (int foundRow : foundRows) {
					int absPos = getAbsoluteRowPosition(foundPage, foundRow);
					for (String colName : colNameValue.keySet()) {
						BitmapIndex index = colNameIndex.get(colName);
						if (index != null) {
							index.update((Comparable)colNameValue.get(colName), absPos);
							try {
								index.save(name, colName);
							} catch (IOException e) {
								System.out.println("Failed to save index for column " + colName);
							}
						}
					}
				}
			}
		}
		
		if (!found) {
			throw new DBAppException("Row not found");
		}
	}

	public void delete(Hashtable<String, Object> colNameValue) throws DBAppException {
		if (!checkPartialRowCompatibility(colNameValue)) {
			throw new DBAppException("Invalid column values");
		}
		
		boolean found = false;
		
		for (int pageI = 0; pageI < pages.size(); pageI++) {
			Page page = pages.get(pageI);
			try {
				page.load(name + "_" + pageI + ".class");
			} catch (IOException e) {
				throw new DBAppException("IO error");
			}
			
			boolean foundHere = false;
			ArrayList<Integer> foundRows = new ArrayList<>();
			
			for (int rowI = 0; rowI < page.data.size(); rowI++) {
				//colNameValue.put("TouchDate", page.data.get(rowI).get("TouchDate"));
				//if (page.data.get(rowI).equals(colNameValue)) {
				if (checkPartialEquality(page.data.get(rowI), colNameValue)) {
					page.data.remove(rowI);
					found = true;
					foundHere = true;
					foundRows.add(rowI);
					rowI--;
					if (page.data.size() == 0) {
						pages.remove(pageI);
					}
				}
			}
			
			boolean nowEmpty = false;
			if (foundHere) {
				if (page.data.size() == 0) {
					shiftPageIndicesAt(pageI + 1, false);
					nowEmpty = true;
				} else {
					try {
						page.save(name + "_" + pageI + ".class");
					} catch (IOException e) {
						throw new DBAppException("IO error");
					}
				}
			}
			page.unload();

			if (foundHere) {
				for (int foundRow : foundRows) {
					int absPos = getAbsoluteRowPosition(pageI, foundRow);
					for (String colName : colNameValue.keySet()) {
						BitmapIndex index = colNameIndex.get(colName);
						if (index != null) {
							index.delete(absPos);
							try {
								index.save(name, colName);
							} catch (IOException e) {
								System.out.println("Failed to save index for column " + colName);
							}
						}
					}
				}
			}
			
			if (nowEmpty) {
				pageI--;
			}
		}
		
		if (!found) {
			unloadPages();
			throw new DBAppException("Row not found");
		}

		writeAndUnloadPages();
	}
	
	public int getAbsoluteRowPosition(int pagePos, int rowPos) {
		int index = 0;

		for (int pageI = 0; pageI < pagePos; pageI++) {
			Page page = pages.get(pageI);
			try {
				page.load(name + "_" + pageI + ".class");
			} catch (IOException e) {
				System.out.println("Failed to read page for table " + name);
				return -1;
			}
			
			index += page.data.size();
			
			page.unload();
		}
		
		index += rowPos;
		return index;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple<Integer, Integer> findInsertPosition(String colName, Comparable key) {
		for (int pageI = 0; pageI < pages.size(); pageI++) {
			Page page = pages.get(pageI);
			try {
				page.load(name + "_" + pageI + ".class");
			} catch (IOException e) {
				System.out.println("Failed to read page for table " + name);
				continue;
			}

			for (int rowI = 0; rowI < page.data.size(); rowI++) {
				if (((Comparable) page.data.get(rowI).get(colName)).compareTo(key) >= 0) {
					return new Tuple<Integer, Integer>(pageI, rowI);
				}
			}
		}
		
		
		Tuple<Integer, Integer> val;

		if (pages.size() == 0) {
			val = new Tuple<Integer, Integer>(0, 0);
		} else {
			val = new Tuple<Integer, Integer>(pages.size() - 1, pages.get(pages.size() - 1).data.size());
		}

		unloadPages();
		return val;
	}
	
	public boolean checkPartialEquality(Hashtable<String, Object> full, Hashtable<String, Object> search) {
		for (String rowName : search.keySet()) {
			//if (!rowName.equals(clusteringKeyColumn) && !search.get(rowName).equals(full.get(rowName))) {
			if (!search.get(rowName).equals(full.get(rowName))) {
				return false;
			}
		}
		return true;
	}
	
	public boolean checkRowCompatibility(Hashtable<String, Object> colNameValue) {
		if (colNameType.size() != colNameValue.size()) {
			return false;
		}
		
		for (String colName : colNameType.keySet()) {
			if (!colNameValue.containsKey(colName)) {
				return false;
			}
			if (!colNameValue.get(colName).getClass().getName().equals(colNameType.get(colName))) {
				return false;
			}
		}
		
		return true;
	}
	
	public boolean checkPartialRowCompatibility(Hashtable<String, Object> colNameValue) {
		for (String colName : colNameValue.keySet()) {
			if (!colNameType.containsKey(colName)) {
				return false;
			}
			if (!colNameValue.get(colName).getClass().getName().equals(colNameType.get(colName))) {
				return false;
			}
		}
		
		return true;
	}
	
	public BitSet select(SQLTerm sqlTerm) throws DBAppException {
		BitmapIndex index = colNameIndex.get(sqlTerm._strColumnName);
		if (index == null) {
			BitSet bitset = new BitSet();
			
			int rowIndex = 0;
			for (int pageI = 0; pageI < pages.size(); pageI++) {
				Page page = pages.get(pageI);
				
				try {
					page.load(name + "_" + pageI + ".class");
				} catch (IOException e) {
					System.out.println("Failed to read page for table " + name);
					return new BitSet();
				}

				for (Hashtable<String, Object> row : page.data) {
					int compare = ((Comparable)row.get(sqlTerm._strColumnName)).compareTo(sqlTerm._objValue);
					boolean matching = false;
					
					if (sqlTerm._strOperator.equals("=") && compare == 0) {
						matching = true;
					} else if (sqlTerm._strOperator.equals("!=") && compare != 0) {
						matching = true;
					} else if (sqlTerm._strOperator.equals(">") && compare > 0) {
						matching = true;
					} else if (sqlTerm._strOperator.equals(">=") && compare >= 0) {
						matching = true;
					} else if (sqlTerm._strOperator.equals("<") && compare < 0) {
						matching = true;
					} else if (sqlTerm._strOperator.equals("<=") && compare <= 0) {
						matching = true;
					}
					
					if (matching) {
						bitset.set(rowIndex);
					}
					
					rowIndex++;
				}
				
				page.unload();
			}
			
			return bitset;
		} else {
			return index.find((Comparable)sqlTerm._objValue, sqlTerm._strOperator);
		}
	}
}

class Tuple<X, Y> implements Serializable {
	public X x;
	public Y y;

	public Tuple(X x, Y y) {
		this.x = x;
		this.y = y;
	}
}
