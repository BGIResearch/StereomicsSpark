package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class AdapterFilter implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String adapterFile;
	private Set<String> adapters;
	
	public AdapterFilter(String adapterFile) {
		this.adapterFile = adapterFile;
		this.adapters = new HashSet<String>();
		this.parseAdapterFile();
	}

	private void parseAdapterFile() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(this.adapterFile));
			String line;
			while((line = reader.readLine()) != null){
				if (line.startsWith(">")) {
					continue;
				}else {
					this.adapters.add(line.trim());
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {			
			e.printStackTrace();
		}catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public boolean findAdapterSeq(String sequence) {
		for (String adapter: this.adapters) {
			if (sequence.contains(adapter)) {
				return true;
			}
		}
		return false;
	}
	
}
