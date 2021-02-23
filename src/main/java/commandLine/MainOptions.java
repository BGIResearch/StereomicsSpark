package commandLine;

import org.apache.commons.cli.Options;

public class MainOptions {
	
	private Options options;
	
	public MainOptions() {
		this.options = new Options();
		this.setOptions();
	}
	
	public Options getOptions() {
		return options;
	}

	public void setOptions() {
		this.options.addOption("m", "mask", true, "stereomics chip mask file.");
		this.options.addOption("r1", "read1", true, "fastq file of the second sequencing read1.");
		this.options.addOption("r2", "read2", true, "fastq file of the second sequencing read2.");
		this.options.addOption("o", "out", true, "output file path.");
		this.options.addOption("stat", true, "statistic file path.");
		this.options.addOption("barcodeStart", true, "barcode start position in the read sequence, 0-based indexing.");
		this.options.addOption("barcodeLen", true, "barcode length.");
		this.options.addOption("umiStart", true, "umi start position in the read sequence, 0-based indexing.");
		this.options.addOption("umiLen", true, "umi length.");
		this.options.addOption("barcodeRead", true, "1 means barcode sequence locates in the read1 and 2 means barcode sequence locates in the read2.");
		this.options.addOption("umiRead", true, "1 means umi sequence locates in the read1 and 2 means umi sequence locates in the read2");
		this.options.addOption("h", "help", false, "list short help");
	}

}
