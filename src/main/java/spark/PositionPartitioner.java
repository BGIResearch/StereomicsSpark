package spark;

import org.apache.spark.Partitioner;

import structures.Position;

public class PositionPartitioner extends Partitioner{
	
	protected int partitionNum;
	protected int axis = 0;
	
	public int getAxis() {
		return axis;
	}

	public void setAxis(int axis) {
		this.axis = axis;
	}

	public PositionPartitioner(int partitionNum) {
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
		if (!(key instanceof Position)) throw new IllegalArgumentException("The barcode should be Position but got: " + key.getClass());
		Position newKey = (Position) key;
		if (this.axis == 0) {
			return newKey.getX()%this.partitionNum;
		}else {
			return newKey.getY()%this.partitionNum;
		}
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return this.partitionNum;
	}


}
