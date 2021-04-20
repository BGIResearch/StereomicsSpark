package structures;

import java.io.Serializable;

public class Position implements Serializable, Comparable<Position>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int X;
	private int Y;
	
	public Position(int x, int y) {
		this.X = x;
		this.Y = y;
	}

	public int getX() {
		return X;
	}

	@Override
	public String toString() {
		return X + "_" + Y;
	}
	
	
	
	public void setX(int x) {
		this.X = x;
	}

	public int getY() {
		return Y;
	}

	public void setY(int y) {
		this.Y = y;
	}

	@Override
	public int compareTo(Position o) {
		if (this.Y == o.Y) {
			return this.X - o.X;
		}else {
			return this.Y - o.Y;
		}
	}
	
}
