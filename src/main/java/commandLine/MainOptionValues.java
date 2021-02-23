package commandLine;

public class MainOptionValues {
	
	private String maskFile;
	private String read1File;
	private String read2File;
	private String outFile;
	private String statFile;
	private int barcodeStart;
	private int barcodeLen;
	private int umiStart;
	private int umiLen;
	private int barcodeRead;
	private int umiRead;
	
	public MainOptionValues() {
		super();
		// TODO Auto-generated constructor stub
	}

	public String getMaskFile() {
		return maskFile;
	}

	public void setMaskFile(String maskFile) {
		this.maskFile = maskFile;
	}

	public String getRead1File() {
		return read1File;
	}

	public void setRead1File(String read1File) {
		this.read1File = read1File;
	}

	public String getRead2File() {
		return read2File;
	}

	public void setRead2File(String read2File) {
		this.read2File = read2File;
	}

	public String getOutFile() {
		return outFile;
	}

	public void setOutFile(String outFile) {
		this.outFile = outFile;
	}

	public String getStatFile() {
		return statFile;
	}

	public void setStatFile(String statFile) {
		this.statFile = statFile;
	}

	public int getBarcodeStart() {
		return barcodeStart;
	}

	public void setBarcodeStart(int barcodeStart) {
		this.barcodeStart = barcodeStart;
	}

	public int getBarcodeLen() {
		return barcodeLen;
	}

	public void setBarcodeLen(int barcodeLen) {
		this.barcodeLen = barcodeLen;
	}

	public int getUmiStart() {
		return umiStart;
	}

	public void setUmiStart(int umiStart) {
		this.umiStart = umiStart;
	}

	public int getUmiLen() {
		return umiLen;
	}

	public void setUmiLen(int umiLen) {
		this.umiLen = umiLen;
	}

	public int getBarcodeRead() {
		return barcodeRead;
	}

	public void setBarcodeRead(int barcodeRead) {
		this.barcodeRead = barcodeRead;
	}

	public int getUmiRead() {
		return umiRead;
	}

	public void setUmiRead(int umiRead) {
		this.umiRead = umiRead;
	}

}
