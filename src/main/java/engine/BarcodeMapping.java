package engine;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import structures.Position;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import fastq.MGIFastqInputFormat;
import fastq.MGISequencedFragment;
import fastq.MGIFastqOutputFormat;
import utils.Utils;

public class BarcodeMapping implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected SparkSession spark;
	protected String maskFile;
	protected String read1File;
	protected String read2File;
	protected String combinedReadsFile;
	protected int barcodeStart = 0;
	protected int barcodeLen = 25;
	protected int umiStart = 25;
	protected int umiLen = 10;
	protected int barcodeRead = 1;
	protected int umiRead = 1;
	protected long totalBarcode = 0;
	protected long totalReads = 0;
	protected long umiFilteredReads = 0;
	protected long mappedReads = 0;
	
	public BarcodeMapping(String maskFile, String read1File, String read2File, String combinedReadsFile) {
		super();
		this.maskFile = maskFile;
		this.read1File = read1File;
		this.read2File = read2File;
		this.combinedReadsFile = combinedReadsFile;
	}
	
	
	
	public BarcodeMapping(String maskFile, String read1File, String read2File, String combinedReadsFile,
			int barcodeStart, int barcodeLen, int umiStart, int umiLen, int barcodeRead,
			int umiRead) {
		super();
		this.maskFile = maskFile;
		this.read1File = read1File;
		this.read2File = read2File;
		this.combinedReadsFile = combinedReadsFile;
		this.barcodeStart = barcodeStart;
		this.barcodeLen = barcodeLen;
		this.umiStart = umiStart;
		this.umiLen = umiLen;
		this.barcodeRead = barcodeRead;
		this.umiRead = umiRead;
	}



	public void run(SparkSession spark) {
		System.out.println(maskFile);
		System.out.println(read1File);
		System.out.println(read2File);
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		LongAccumulator combinedReadsCount = spark.sparkContext().longAccumulator("combinedReads");
		LongAccumulator umiFilteredReadsCount = spark.sparkContext().longAccumulator("umiFilteredReads");
		LongAccumulator inputBarcodesCount = spark.sparkContext().longAccumulator("inputBarcodes");
		JavaPairRDD<Long, Position> bpMapRdd = loadChipMaskFile(sc, inputBarcodesCount);
		JavaPairRDD<Long, Tuple2<Text, MGISequencedFragment>> combinedReadsRdd = loadAndCombineFastq(sc, combinedReadsCount);
		JavaPairRDD<Long, Tuple2<Tuple2<Text, MGISequencedFragment>, Position>> mappedReadsRdd = combinedReadsRdd.join(bpMapRdd);
		JavaPairRDD<Text, MGISequencedFragment> outputReadsRdd = mappedReadsRdd.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<Text,MGISequencedFragment>,Position>>, Text, MGISequencedFragment>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<Text, MGISequencedFragment> call(
					Tuple2<Long, Tuple2<Tuple2<Text, MGISequencedFragment>, Position>> t) throws Exception {
				t._2._1._2.setCoordinate(t._2._2.toString());;
				Tuple2<Text, MGISequencedFragment> read = new Tuple2<Text, MGISequencedFragment>(t._2._1._1, t._2._1._2);			
				return read;
			}		
		});
		outputReadsRdd.saveAsNewAPIHadoopFile(combinedReadsFile, Text.class, MGISequencedFragment.class, MGIFastqOutputFormat.class);
		System.out.printf("Input barcodes number: %d\n", inputBarcodesCount.value());
		System.out.printf("Total Inpute reads number: %d\n", combinedReadsCount.value());
	}
	
	public JavaPairRDD<Long, Position> loadChipMaskFile(JavaSparkContext sc, final LongAccumulator inputBarcodesCount){
		JavaRDD<String> maskRdd = sc.textFile(maskFile);
		JavaPairRDD<Long, Position> bpMapRdd = maskRdd.mapToPair(new PairFunction<String, Long, Position>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Position> call(String line) throws Exception {
				inputBarcodesCount.add(1);
				String[] nums = line.trim().split("\t");
				Position position = new Position(Integer.parseInt(nums[1]), Integer.parseInt(nums[2]));
				return new Tuple2<Long, Position>(Long.parseLong(nums[0]), position);
			}			
		}).reduceByKey((v1, v2) -> null).filter(v -> v._2 != null);
		return bpMapRdd;
	}
	
	public JavaPairRDD<Long, Tuple2<Text, MGISequencedFragment>> loadAndCombineFastq(JavaSparkContext sc, final LongAccumulator combinedReadsCount){
		Configuration conf = sc.hadoopConfiguration();
		JavaPairRDD<Text, MGISequencedFragment> read1Rdd = sc.newAPIHadoopFile(this.read1File, MGIFastqInputFormat.class, Text.class, MGISequencedFragment.class, conf);
		JavaPairRDD<Text, MGISequencedFragment> read2Rdd = sc.newAPIHadoopFile(this.read2File, MGIFastqInputFormat.class, Text.class, MGISequencedFragment.class, conf);
		JavaPairRDD<Text, Tuple2<MGISequencedFragment, MGISequencedFragment>> readsRdd = read1Rdd.join(read2Rdd);
		JavaPairRDD<Long, Tuple2<Text, MGISequencedFragment>> combinedReadsRdd = readsRdd.mapToPair(new PairFunction<Tuple2<Text, Tuple2<MGISequencedFragment,MGISequencedFragment>>, Long, Tuple2<Text, MGISequencedFragment>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Tuple2<Text, MGISequencedFragment>> call(
					Tuple2<Text, Tuple2<MGISequencedFragment, MGISequencedFragment>> reads) throws Exception {
				combinedReadsCount.add(1);
				String barcode;
				String umi;
				String umiQuality;
				if (barcodeRead==1) {
					barcode = reads._2._1.getSequence().toString().substring(barcodeStart, barcodeStart+barcodeLen);
				}else {
					barcode = reads._2._2.getSequence().toString().substring(barcodeStart, barcodeStart+barcodeLen);
				}
				if (umiRead==1) {
					umi = reads._2._1.getSequence().toString().substring(umiStart, umiStart+umiLen);
					umiQuality = reads._2._1.getQuality().toString().substring(umiStart, umiStart+umiLen);
				}else {
					umi = reads._2._2.getSequence().toString().substring(umiStart, umiStart+umiLen);
					umiQuality = reads._2._2.getQuality().toString().substring(umiStart, umiStart+umiLen);
				}
				long barcodeInt = Utils.encode(barcode);
				reads._2._2.setUmi(umi);
				reads._2._2.setUmiQual(umiQuality);
				Tuple2<Text, MGISequencedFragment> newValue = new Tuple2<Text, MGISequencedFragment>(reads._1, reads._2._2);
				return new Tuple2<Long, Tuple2<Text, MGISequencedFragment>>(barcodeInt, newValue);
			}			
		});
		return combinedReadsRdd;
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

}
