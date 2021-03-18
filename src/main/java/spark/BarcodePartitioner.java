package spark;

import org.apache.spark.Partitioner;

public class BarcodePartitioner extends Partitioner{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int partitionNum;
	
	public BarcodePartitioner(int partitionNum) {
		this.partitionNum = partitionNum;
	}

	
	public int getPartitionNum() {
		return partitionNum;
	}


	public void setPartitionNum(int partitionNum) {
		this.partitionNum = partitionNum;
	}


	@Override
	public int getPartition(Object key) {
		if (!(key instanceof Long)) throw new IllegalArgumentException("The barcode should be Long but got: " + key.getClass());
		
		return (int)((long)key%this.numPartitions());
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return this.partitionNum;
	}

}
