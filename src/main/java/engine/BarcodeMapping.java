package engine;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import spark.BarcodeComparator;
import spark.BarcodePartitioner;
import spark.PositionPartitioner;
import structures.Position;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import fastq.MGISequencedFragment;
import fastq.MgzFastqInputFormat;
import fastq.MGIFastqInputFormat;
import fastq.MGIFastqOutputFormat;
import utils.Utils;
import utils.AdapterFilter;

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
	protected String adapterFile;
	protected int barcodeStart = 0;
	protected int barcodeLen = 25;
	protected int umiStart = 25;
	protected int umiLen = 10;
	protected int barcodeRead = 1;
	protected int umiRead = 1;
	protected long totalBarcode = 0;
	protected long adapterFilteredReads = 0;
	protected long uniqueBarcode = 0;
	protected long totalReads = 0;
	protected long totalreadsBase = 0;
	protected long umiFilteredReads = 0;
	protected long mappedReads = 0;
	protected long barcodeQ30Base = 0;
	protected long umiQ30Base = 0;
	protected long readQ30Base = 0;
	private int outPartition;
	
	public BarcodeMapping(String maskFile, String read1File, String read2File, String combinedReadsFile) {
		super();
		this.maskFile = maskFile;
		this.read1File = read1File;
		this.read2File = read2File;
		this.combinedReadsFile = combinedReadsFile;
	}
	
	
	
	public BarcodeMapping(String maskFile, String read1File, String read2File, String combinedReadsFile,
			int barcodeStart, int barcodeLen, int umiStart, int umiLen, int barcodeRead,
			int umiRead, int outPartition) {
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
		this.outPartition = outPartition;
	}



	public void run(SparkSession spark) {
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		LongAccumulator combinedReadsCount = spark.sparkContext().longAccumulator("combinedReads");
		LongAccumulator adapterFilteredReadsCount = spark.sparkContext().longAccumulator("adapterFilteredReads");
		LongAccumulator umiFilteredReadsCount = spark.sparkContext().longAccumulator("umiFilteredReads");
		LongAccumulator inputBarcodesCount = spark.sparkContext().longAccumulator("inputBarcodes");
		LongAccumulator mappedReadsCount = spark.sparkContext().longAccumulator("mappedReads");
		LongAccumulator umiQ30BaseCount = spark.sparkContext().longAccumulator("umiQ30Bases");
		LongAccumulator barcodeQ30BaseCount = spark.sparkContext().longAccumulator("barcodeQ30Bases");
		LongAccumulator readQ30BaseCount = spark.sparkContext().longAccumulator("readQ30Bases");
		LongAccumulator totalReadBaseCount = spark.sparkContext().longAccumulator("totalReadBaseCount");
		
		JavaPairRDD<Long, Position> bpMapRdd = null;
		if (maskFile.endsWith(".txt")) {
			bpMapRdd = loadTxtChipMaskFile(sc, inputBarcodesCount, outPartition);
		}else if (maskFile.endsWith(".bin")) {
			bpMapRdd = loadBinChipMaskFile(sc, inputBarcodesCount, outPartition);
		}else {
			System.exit(1);
		}
		
		int fastqJoinPartition = outPartition > 0? outPartition : bpMapRdd.getNumPartitions();
		JavaPairRDD<Long, Tuple2<Text, MGISequencedFragment>> combinedReadsRdd = loadAndCombineFastq(sc, fastqJoinPartition, 
				combinedReadsCount, umiFilteredReadsCount, adapterFilteredReadsCount, umiQ30BaseCount, barcodeQ30BaseCount, 
				readQ30BaseCount, totalReadBaseCount);
		
		/**
		JavaPairRDD<Long, Integer> capturedBarcodes = combinedReadsRdd.mapToPair(v -> new Tuple2<Long, Integer>(v._1, 1)).reduceByKey((v1, v2) -> v1+v2);
		JavaPairRDD<Long, Tuple2<Integer, Optional<Position>>> capturedBpMapRdd = capturedBarcodes.leftOuterJoin(bpMapRdd);
		BarcodeComparator barcodeComparator = new BarcodeComparator();
		JavaPairRDD<Long, Optional<Position>> sortedCapturedBpMapRdd = capturedBpMapRdd.mapToPair(v -> new Tuple2<Long, Optional<Position>>(v._1, v._2._2))
				.sortByKey(barcodeComparator);
		sortedCapturedBpMapRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Long, Optional<Position>>>, Long, Position>(){

			
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<Long, Position>> call(Iterator<Tuple2<Long, Optional<Position>>> t)
					throws Exception {
				List<Tuple2<Long, Position>> result = new ArrayList<Tuple2<Long, Position>>();
				Stack<Long> neighborBarcodes = new Stack<Long>();
				Tuple2<Long, Optional<Position>> pre = t.next();
				neighborBarcodes.push(pre._1);
				Position currPos = pre._2.isPresent() ? pre._2.get() : null;
				Tuple2<Long, Optional<Position>> curr;
				while(t.hasNext()) {
					curr = t.next();
					Long top = neighborBarcodes.lastElement();
					if (barcodeComparator.compare(curr._1, top)!=0) {
						if (currPos != null && curr._2.isPresent())
						neighborBarcodes.push(curr._1);
					}
				}
				return null;
			}
			
		});
		
		
		JavaPairRDD<Long, Position> mappedBarcodes = capturedBpMapRdd.filter(v -> v._2._2.isPresent()).mapToPair(v -> new Tuple2<Long, Position>(v._1, v._2._2.get()));
		JavaPairRDD<Long, Integer> unMappedBarcodes = capturedBpMapRdd.filter(v -> !v._2._2.isPresent()).mapToPair(v -> new Tuple2<Long, Integer>(v._1, 1));
		*/
		
		JavaPairRDD<Long, Tuple2<Tuple2<Text, MGISequencedFragment>, Position>> mappedReadsRdd = combinedReadsRdd.join(bpMapRdd);
				//.filter(v -> (v !=null && v._2._1 !=null && v._2._2 != null));
		
		JavaPairRDD<Text, MGISequencedFragment> outputReadsRdd = mappedReadsRdd
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<Text, MGISequencedFragment>, Position>>, Text, MGISequencedFragment>(){

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Text, MGISequencedFragment> call(
							Tuple2<Long, Tuple2<Tuple2<Text, MGISequencedFragment>, Position>> t) throws Exception {
						mappedReadsCount.add(1);
						t._2._1._2.setCoordinate(t._2._2.toString());
						return t._2._1;
					}				
				});//.persist(StorageLevel.MEMORY_AND_DISK_SER());
		Configuration conf = sc.hadoopConfiguration();
		if (combinedReadsFile.endsWith(".gz")) {
			conf.set(FileOutputFormat.COMPRESS,"true");
			conf.set(FileOutputFormat.COMPRESS_CODEC,GzipCodec.class.getName());
		}
		if (outPartition>0) {
			//outputReadsRdd = outputReadsRdd.repartition(outPartition);
			outputReadsRdd = outputReadsRdd.coalesce(outPartition);
		}
		
		outputReadsRdd.saveAsNewAPIHadoopFile(combinedReadsFile, Text.class, MGISequencedFragment.class, MGIFastqOutputFormat.class, conf);
		this.totalBarcode = inputBarcodesCount.value();
		this.totalReads = combinedReadsCount.value();
		this.adapterFilteredReads = adapterFilteredReadsCount.value();
		this.umiFilteredReads = umiFilteredReadsCount.value();
		this.mappedReads = mappedReadsCount.value();
		this.barcodeQ30Base = barcodeQ30BaseCount.value();
		this.umiQ30Base = umiQ30BaseCount.value();
		this.readQ30Base = readQ30BaseCount.value();
		this.totalreadsBase = totalReadBaseCount.value();
	}
	
	public JavaPairRDD<Long, Position> loadBinChipMaskFile(JavaSparkContext sc, final LongAccumulator inputBarcodesCount, int partition){
		
		//Configuration conf = sc.hadoopConfiguration();
		//if (partition > 0) {
		//	conf.setInt("mapred.max.split.size", partition);
		//}
		JavaRDD<byte[]> maskRdd = sc.binaryRecords(maskFile, 16);
		
		JavaPairRDD<Long, Position> bpMapRdd = maskRdd.mapToPair(new PairFunction<byte[], Long, Position>(){

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Position> call(byte[] record) throws Exception {
				inputBarcodesCount.add(1);				
				byte[] barcodeBytes = {record[7], record[6], record[5], record[4], record[3], record[2], record[1], record[0]};
				byte[] xBytes = {record[11], record[10], record[9], record[8]};
				byte[] yBytes = {record[15], record[14], record[13], record[12]};
				
				ByteBuffer barcodeBuffer = ByteBuffer.allocate(8);
				ByteBuffer xBuffer = ByteBuffer.allocate(4);
				ByteBuffer yBuffer = ByteBuffer.allocate(4);
				
				barcodeBuffer.put(barcodeBytes, 0, 8);
				xBuffer.put(xBytes, 0, 4);
				yBuffer.put(yBytes, 0, 4);
				
				barcodeBuffer.flip();
				xBuffer.flip();
				yBuffer.flip();
				
				Long barcodeInt = barcodeBuffer.getLong();
				int x = xBuffer.getInt();
				int y = yBuffer.getInt();
				//System.out.printf("%d\t%d\t%d\n", barcodeInt, x, y);
				Position position = new Position(x, y);
				return new Tuple2<Long, Position>(barcodeInt, position);
			}		
		});//.partitionBy(new BarcodePartitioner(partition)).persist(StorageLevel.MEMORY_AND_DISK_SER());
		//bpMapRdd = partition > 0 ? bpMapRdd.reduceByKey((v1, v2) -> null, partition).filter(v -> v!=null).partitionBy(new BarcodePartitioner(bpMapRdd.getNumPartitions())) 
		//		: bpMapRdd.reduceByKey((v1, v2) -> null).filter(v -> v!=null).partitionBy(new BarcodePartitioner(bpMapRdd.getNumPartitions()));		
		//this.uniqueBarcode = bpMapRdd.count();
		return bpMapRdd;
	}
	
	public JavaPairRDD<Long, Position> loadTxtChipMaskFile(JavaSparkContext sc, final LongAccumulator inputBarcodesCount, int partition){
		
		JavaRDD<String> maskRdd = sc.textFile(maskFile);
		JavaPairRDD<Long, Position> bpMapRdd = maskRdd.mapToPair(new PairFunction<String, Long, Position>(){

			
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Position> call(String line) throws Exception {
				inputBarcodesCount.add(1);
				String[] nums = line.trim().split("\t");
				Position position = new Position(Integer.parseInt(nums[1]), Integer.parseInt(nums[2]));
				return new Tuple2<Long, Position>(Long.parseLong(nums[0]), position);
			}			
		});
		
		
		return bpMapRdd;
	}
	
	public JavaPairRDD<Long, Tuple2<Text, MGISequencedFragment>> loadAndCombineFastq(JavaSparkContext sc, int partitionNum, final LongAccumulator combinedReadsCount, 
			final LongAccumulator umiFilteredReadsCount, final LongAccumulator adapterFilteredReadsCount, final LongAccumulator umiQ30BaseCount,
			final LongAccumulator barcodeQ30BaseCount, final LongAccumulator readQ30BaseCount, final LongAccumulator readBaseCount){
		Configuration conf = sc.hadoopConfiguration();
		File fq1 = new File(read1File);
		String fq1Name = fq1.getName();
		JavaPairRDD<Text, MGISequencedFragment> read1Rdd;
		JavaPairRDD<Text, MGISequencedFragment> read2Rdd;
		
		if (fq1Name.endsWith("gz")){
			read1Rdd = sc.newAPIHadoopFile(this.read1File, MgzFastqInputFormat.class, Text.class, MGISequencedFragment.class, conf);
			read2Rdd = sc.newAPIHadoopFile(this.read2File, MgzFastqInputFormat.class, Text.class, MGISequencedFragment.class, conf);
		}else {
			read1Rdd = sc.newAPIHadoopFile(this.read1File, MGIFastqInputFormat.class, Text.class, MGISequencedFragment.class, conf);
			read2Rdd = sc.newAPIHadoopFile(this.read2File, MGIFastqInputFormat.class, Text.class, MGISequencedFragment.class, conf);
		}
		JavaPairRDD<Text, Tuple2<MGISequencedFragment, MGISequencedFragment>> readsRdd = read1Rdd.join(read2Rdd);
		final AdapterFilter adapterFilter = this.adapterFile!=null ? new AdapterFilter(this.adapterFile): null;
		
		JavaPairRDD<Long, Tuple2<Text, MGISequencedFragment>> combinedReadsRdd = readsRdd.mapToPair(new PairFunction<Tuple2<Text, Tuple2<MGISequencedFragment,MGISequencedFragment>>, Long, Tuple2<Text, MGISequencedFragment>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Tuple2<Text, MGISequencedFragment>> call(
					Tuple2<Text, Tuple2<MGISequencedFragment, MGISequencedFragment>> reads) throws Exception {
				combinedReadsCount.add(1);
				String umi;
				String umiQuality;
				long barcodeInt;
				String read1Seq = reads._2._1.getSequence().toString();
				String read1Qual = reads._2._1.getQuality().toString();
				String read2Seq = reads._2._2.getSequence().toString();
				String read2Qual = reads._2._2.getQuality().toString();
				
				if (adapterFilter != null && adapterFilter.findAdapterSeq(read2Seq)) {
					adapterFilteredReadsCount.add(1);
					return null;
				}
				readBaseCount.add(read2Seq.length());
				if (barcodeRead==1) {
					barcodeInt = Utils.encode(read1Seq, barcodeStart, barcodeLen);
					barcodeQ30BaseCount.add(Utils.seqQualityStat(read1Qual, barcodeStart, barcodeLen));
				}else {
					barcodeInt = Utils.encode(read2Seq, barcodeStart, barcodeLen);
					barcodeQ30BaseCount.add(Utils.seqQualityStat(read2Qual, barcodeStart, barcodeLen));
				}
				if (umiRead==1) {
					umi = read1Seq.substring(umiStart, umiStart+umiLen);
					umiQuality = read1Qual.substring(umiStart, umiStart+umiLen);
				}else {
					umi = read2Seq.substring(umiStart, umiStart+umiLen);
					umiQuality = read2Qual.substring(umiStart, umiStart+umiLen);
				}
				readQ30BaseCount.add(Utils.seqQualityStat(read2Qual, 0, read2Qual.length()));
				umiQ30BaseCount.add(Utils.seqQualityStat(umiQuality, 0, umiLen));
				if (!Utils.umiQCpass(umi, umiQuality)) {
					umiFilteredReadsCount.add(1);
					return null;
				}			
				reads._2._2.setUmi(umi);
				reads._2._2.setUmiQual(umiQuality);
				Tuple2<Text, MGISequencedFragment> newValue = new Tuple2<Text, MGISequencedFragment>(reads._1, reads._2._2);
				return new Tuple2<Long, Tuple2<Text, MGISequencedFragment>>(barcodeInt, newValue);
			}			
		}).filter(v -> v!=null);//.persist(StorageLevel.MEMORY_AND_DISK_SER());//.partitionBy(new BarcodePartitioner(partitionNum));
		return combinedReadsRdd;
	}
	
	public void BarcodeMappingStat(String statFilePath) {
		long cleanReads = this.totalReads - this.adapterFilteredReads;
		try {
			BufferedWriter statWriter = new BufferedWriter(new FileWriter(statFilePath));
			statWriter.write(String.format("getBarcodePositionMap_uniqBarcodeTypes:\t%d\n", this.totalBarcode));
			//statWriter.write(String.format("unique input barcode number: %d\n", this.uniqueBarcode));
			statWriter.write(String.format("total_reads:\t%d\n", this.totalReads));
			statWriter.write(String.format("adapter_filtered_reads:\t%d\t%.2f", this.adapterFilteredReads,  
					(double)this.adapterFilteredReads/this.totalReads*100) + "%\n");
			statWriter.write(String.format("umi_filtered_reads:\t%d\t%.2f", this.umiFilteredReads, 
					(double)this.umiFilteredReads/cleanReads*100) + "%\n");
			statWriter.write(String.format("mapped_reads:\t%d\t%.2f", this.mappedReads,
					(double)this.mappedReads/(this.totalReads-this.adapterFilteredReads-this.umiFilteredReads)*100) + "%\n");
			statWriter.write(String.format("Q30_bases_in_barcode:\t%.2f", (double)this.barcodeQ30Base/(cleanReads*this.barcodeLen)*100) + "%\n");
			statWriter.write(String.format("Q30_bases_in_umi:\t%.2f", (double)this.umiQ30Base/(cleanReads*this.umiLen)*100) + "%\n");
			statWriter.write(String.format("Q30_bases_in_read:\t%.2f", (double)this.readQ30Base/(this.totalreadsBase)*100) + "%\n");
			statWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String getAdapterFile() {
		return adapterFile;
	}



	public void setAdapterFile(String adapterFile) {
		this.adapterFile = adapterFile;
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



	public String getCombinedReadsFile() {
		return combinedReadsFile;
	}



	public void setCombinedReadsFile(String combinedReadsFile) {
		this.combinedReadsFile = combinedReadsFile;
	}



	public int getUmiStart() {
		return umiStart;
	}



	public void setUmiStart(int umiStart) {
		this.umiStart = umiStart;
	}



	public long getTotalBarcode() {
		return totalBarcode;
	}



	public void setTotalBarcode(long totalBarcode) {
		this.totalBarcode = totalBarcode;
	}



	public long getTotalReads() {
		return totalReads;
	}



	public void setTotalReads(long totalReads) {
		this.totalReads = totalReads;
	}



	public long getUmiFilteredReads() {
		return umiFilteredReads;
	}



	public void setUmiFilteredReads(long umiFilteredReads) {
		this.umiFilteredReads = umiFilteredReads;
	}



	public long getMappedReads() {
		return mappedReads;
	}



	public void setMappedReads(long mappedReads) {
		this.mappedReads = mappedReads;
	}



	public long getUniqueBarcode() {
		return uniqueBarcode;
	}



	public void setUniqueBarcode(long uniqueBarcode) {
		this.uniqueBarcode = uniqueBarcode;
	}



	public int getOutPartition() {
		return outPartition;
	}



	public void setOutPartition(int outPartition) {
		this.outPartition = outPartition;
	}
	

}
