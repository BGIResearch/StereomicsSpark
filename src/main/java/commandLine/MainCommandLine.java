package commandLine;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class MainCommandLine {
	
	String[] args;
	Options options;
	CommandLine cl;
	MainOptionValues optionValues;
	
	public MainCommandLine(MainOptions options, String[] args) {
		this.args = args;
		this.options = options.getOptions();
		this.optionValues = new MainOptionValues();
		this.parseCommandLine();
	}
	
	public void parseCommandLine() {
		CommandLineParser parser = new PosixParser();
		try {
			this.cl = parser.parse(this.options, this.args);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (cl.hasOption("h") || !cl.hasOption("m") || !cl.hasOption("r1") || !cl.hasOption("r2") || !cl.hasOption("o")) {
			HelpFormatter hf  = new HelpFormatter();
			hf.printHelp("map second sequencing barcode of the stereo_seq to stereo_chip.",  options);
			System.exit(1);
		}else {
			this.setOptionValues();
		}
	}
	
	public String getStringValue(String option, String defaultValue) {
		return cl.hasOption(option) ? cl.getOptionValue(option) : defaultValue;
	}
	
	public String getStringValue(String option) {
		return cl.hasOption(option) ? cl.getOptionValue(option) : null;
	}
	
	public int getIntValue(String option, int defaultValue) {
		return cl.hasOption(option) ? Integer.parseInt(cl.getOptionValue(option)): defaultValue;
	}
	
	private void setOptionValues() {
		optionValues.setMaskFile(this.getStringValue("m"));
		optionValues.setRead1File(this.getStringValue("r1"));
		optionValues.setRead2File(this.getStringValue("r2"));
		optionValues.setOutFile(this.getStringValue("o"));
		optionValues.setStatFile(this.getStringValue("stat", "statistic.txt"));
		optionValues.setAdapterFile(this.getStringValue("adapterFile"));
		optionValues.setBarcodeStart(this.getIntValue("barcodeStart", 0));
		optionValues.setBarcodeLen(this.getIntValue("barcodeLen", 25));
		optionValues.setBarcodeRead(this.getIntValue("barcodeRead", 1));
		optionValues.setUmiStart(this.getIntValue("umiStart", 25));
		optionValues.setUmiLen(this.getIntValue("umiLen", 10));
		optionValues.setUmiRead(this.getIntValue("umiRead", 1));
		optionValues.setOutPartition(this.getIntValue("outPartition", 0));
	}
	
	public String[] getArgs() {
		return args;
	}
	public void setArgs(String[] args) {
		this.args = args;
	}
	public Options getOptions() {
		return options;
	}
	public void setOptions(Options options) {
		this.options = options;
	}
	public CommandLine getCl() {
		return cl;
	}
	public void setCl(CommandLine cl) {
		this.cl = cl;
	}

	public MainOptionValues getOptionValues() {
		return optionValues;
	}

	public void setOptionValues(MainOptionValues optionValues) {
		this.optionValues = optionValues;
	}

}
