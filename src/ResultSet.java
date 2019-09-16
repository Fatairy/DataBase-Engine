import java.io.IOException;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Iterator;

public class ResultSet implements Iterator {
	public BitSet bitset;
	public Table table;
	
	Hashtable<String, Object> row = null;
	int lastReturnedRow = -1;
	int pageIndex = 0;
	int prevRows = 0;
	
	public ResultSet(Table table, BitSet bitset) {
		this.table = table;
		this.bitset = bitset;
	}

	@Override
	public boolean hasNext() {
		int next = bitset.nextSetBit(lastReturnedRow + 1);
		return next >= 0;
	}

	@Override
	public Object next() {
		int next = bitset.nextSetBit(lastReturnedRow + 1);
		if (next == -1) {
			return null;
		}
		
		while (pageIndex < table.pages.size()) {
			Page page = table.pages.get(pageIndex);
			if (!page.loaded) {
				try {
					page.load(table.name + "_" + pageIndex + ".class");
				} catch (IOException e) {
					System.out.println("Failed to read page for table " + table.name);
					return null;
				}
			}
			
			if (next - prevRows < page.data.size()) {
				lastReturnedRow = next;
				return page.data.get(next - prevRows);
			}

			pageIndex++;
			prevRows += page.data.size();
			page.unload();
		}
		
		return null;
	}
}
