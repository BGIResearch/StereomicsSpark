package spark;

import org.apache.hadoop.io.Text;
import org.apache.spark.Partitioner;
import fastq.MGISequencedFragment;
import scala.Tuple2;

public class MGISequencedFragmentPartitioner extends Partitioner{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int partitionNum;
	
	public MGISequencedFragmentPartitioner(int partitionNum) {
		this.partitionNum = partitionNum;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int getPartition(Object key) {
		if (!(key instanceof Tuple2)) throw new IllegalArgumentException("The barcode should be tuple2<Text, MGISequencedFragment> but got: " + key.getClass());
		key = (Tuple2<Text, MGISequencedFragment>) key;
		return 0;
	}

	@Override
	public int numPartitions() {
		return this.partitionNum;
	}

}
