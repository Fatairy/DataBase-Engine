import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Hashtable;

public class BitmapIndex {
	ArrayList<IndexPage> pages;

	public BitmapIndex() {
		pages = new ArrayList<>();
	}

	public void insert(Comparable key, int index) {
		boolean found = false;
		
		for (IndexPage page : pages) {
			for (Tuple<Comparable, BitSet> tuple : page.bitmap) {
				BitSet bitset = tuple.y;
				for (int bit = bitset.length(); bit > index; bit--) {
					bitset.set(bit, bitset.get(bit - 1));
				}
				
				if (tuple.x.equals(key)) {
					bitset.set(index);
					found = true;
				} else {
					bitset.clear(index);
				}
			}
		}
		
		if (!found) {
			BitSet bitset = new BitSet();
			bitset.set(index);
			
			Tuple<Integer, Integer> pos = findInsertPosition(key);
			if (pos.x >= pages.size()) {
				IndexPage newPage = new IndexPage();
				newPage.bitmap.add(new Tuple<>(key, bitset));
				pages.add(newPage);
			} else {
				pages.get(pos.x).bitmap.add(pos.y, new Tuple<>(key, bitset));
				if (pages.get(pos.x).bitmap.size() > IndexPage.pageMaximum) {
					IndexPage newPage = new IndexPage();
					newPage.bitmap.add(pages.get(pos.x).bitmap.remove(IndexPage.pageMaximum));
					pages.add(pos.x + 1, newPage);
				}
			}
		}
	}

	public void update(Comparable key, int index) {
		delete(index);
		insert(key, index);
	}

	public void delete(int index) {
		for (IndexPage page : pages) {
			for (Tuple<Comparable, BitSet> tuple : page.bitmap) {
				BitSet bitset = tuple.y;
				int last = bitset.length() - 1;
				for (int bit = index + 1; bit < bitset.length(); bit++) {
					bitset.set(bit - 1, bitset.get(bit));
				}
				if (last >= index) {
					bitset.clear(last);
				}
				//tuple.y = bitset.get(0, bitset.length() - 2);
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Tuple<Integer, Integer> findInsertPosition(Comparable key) {
		for (int pageI = 0; pageI < pages.size(); pageI++) {
			IndexPage page = pages.get(pageI);

			for (int rowI = 0; rowI < page.bitmap.size(); rowI++) {
				if (page.bitmap.get(rowI).x.compareTo(key) >= 0) {
					return new Tuple<Integer, Integer>(pageI, rowI);
				}
			}
		}

		Tuple<Integer, Integer> val;

		if (pages.size() == 0) {
			val = new Tuple<Integer, Integer>(0, 0);
		} else {
			val = new Tuple<Integer, Integer>(pages.size() - 1, pages.get(pages.size() - 1).bitmap.size());
		}

		return val;
	}
	
	public BitSet find(Comparable value, String operator) {
		BitSet bitset = new BitSet();
		
		for (IndexPage page : pages) {
			for (Tuple<Comparable, BitSet> row : page.bitmap) {
				if (operator.equals("=")) {
					if (row.x.equals(value)) {
						bitset.or(row.y);
						return bitset;
					}
				} else if (operator.equals("!=")) {
					if (!row.x.equals(value)) {
						bitset.or(row.y);
					}
				} else if (operator.equals(">")) {
					if (row.x.compareTo(value) > 0) {
						bitset.or(row.y);
					}
				} else if (operator.equals(">=")) {
					if (row.x.compareTo(value) >= 0) {
						bitset.or(row.y);
					}
				} else if (operator.equals("<")) {
					if (row.x.compareTo(value) < 0) {
						bitset.or(row.y);
					} else {
						return bitset;
					}
				} else if (operator.equals("<=")) {
					if (row.x.compareTo(value) <= 0) {
						bitset.or(row.y);
					} else {
						return bitset;
					}
				}
			}
		}
		return bitset;
	}
	
	public void load(String tblName, String colName) throws IOException {
		File dir = new File(".");
		File[] files = dir.listFiles();
		IndexPage[] foundPages = new IndexPage[files.length];
		
		for (File file : files) {
			if (file.getName().startsWith("_index_" + tblName + "_" + colName + "_") && file.getName().endsWith(".class")) {
				String fileName = file.getName();
				fileName = fileName.substring(("_index_" + tblName + "_" + colName + "_").length());
				fileName = fileName.substring(0, fileName.length() - 6);
				int pageI = Integer.parseInt(fileName);
				
				foundPages[pageI] = new IndexPage();
				foundPages[pageI].load(file.getName());
			}
		}
		
		pages.clear();
		for (IndexPage page : foundPages) {
			if (page != null) {
				pages.add(page);
			}
		}
	}
	
	public void save(String tblName, String colName) throws IOException {
		int pageI = 0;
		for (IndexPage page : pages) {
			page.save("_index_" + tblName + "_" + colName + "_" + pageI + ".class");
			pageI++;
		}
	}
}
