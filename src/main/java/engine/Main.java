package engine;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import commandLine.MainCommandLine;
import commandLine.MainOptionValues;
import commandLine.MainOptions;


public class Main {
	
	public static void main(String[] args) {
		MainOptions mainOptions = new MainOptions();
		MainCommandLine mainCommandline = new MainCommandLine(mainOptions, args);
		MainOptionValues mOpts = mainCommandline.getOptionValues();
		
		SparkConf conf = new SparkConf();
		conf.setAppName("barcodeMapping").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		//conf.setMaster("local[*]").setAppName("barcodeMapping").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		BarcodeMapping barcodeMapping = new BarcodeMapping(mOpts.getMaskFile(), mOpts.getRead1File(), 
				mOpts.getRead2File(), mOpts.getOutFile(), mOpts.getBarcodeStart(), mOpts.getBarcodeLen(), mOpts.getUmiStart(), mOpts.getUmiLen(), 
				mOpts.getBarcodeRead(), mOpts.getUmiRead(), mOpts.getOutPartition());
		if (mOpts.getAdapterFile()!=null && !mOpts.getAdapterFile().isEmpty()) {
			barcodeMapping.setAdapterFile(mOpts.getAdapterFile());
		}
		barcodeMapping.run(spark);
		barcodeMapping.BarcodeMappingStat(mOpts.getStatFile());
		spark.close();
	}

}
