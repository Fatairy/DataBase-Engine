import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
	private static final long serialVersionUID = -1109962273371934713L;
	
	public static final int pageMaximum = 20;
	
	public transient boolean loaded;
	public Vector<Hashtable<String, Object>> data;
	
	@SuppressWarnings("unchecked")
	public Page() {
		data = null;
		loaded = false;
	}
	
	public void init() {
		data = new Vector<>();
		loaded = true;
	}
	
	public void load(String filename) throws IOException {
		if (!loaded) {
			FileInputStream inputStream = new FileInputStream(filename);
			ObjectInputStream objStream = new ObjectInputStream(inputStream);
			try {
				Page page = (Page)objStream.readObject();
				data = page.data;
			} catch (ClassNotFoundException e) {
				init();
			}
			inputStream.close();
			
			loaded = true;
		}
	}
	
	public void unload() {
		loaded = false;
		data = null;
	}
	
	public void save(String filename) throws IOException {
		if (loaded) {
			FileOutputStream outputStream = new FileOutputStream(filename);
			ObjectOutputStream objOutput = new ObjectOutputStream(outputStream);
			objOutput.writeObject(this);
			outputStream.close();
		}
	}
}
