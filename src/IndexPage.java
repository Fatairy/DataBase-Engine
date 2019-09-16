import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class IndexPage implements Serializable {
	public static final int pageMaximum = 100;

	@SuppressWarnings("rawtypes")
	ArrayList<Tuple<Comparable, BitSet>> bitmap;
	
	public IndexPage() {
		bitmap = new ArrayList<>();
	}
	
	public void load(String filename) throws IOException {
		FileInputStream inputStream = new FileInputStream(filename);
		GZIPInputStream gzipStream = new GZIPInputStream(inputStream);
		ObjectInputStream objStream = new ObjectInputStream(gzipStream);
		try {
			IndexPage page = (IndexPage)objStream.readObject();
			bitmap = page.bitmap;
		} catch (ClassNotFoundException e) {
			bitmap = new ArrayList<>();
		}
		objStream.close();
		gzipStream.close();
		inputStream.close();
	}
	
	public void save(String filename) throws IOException {
		FileOutputStream outputStream = new FileOutputStream(filename);
		GZIPOutputStream gzipStream = new GZIPOutputStream(outputStream);
		ObjectOutputStream objOutput = new ObjectOutputStream(gzipStream);
		objOutput.writeObject(this);
		objOutput.close();
		gzipStream.close();
		outputStream.close();
	}
}
