package spark;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashSet;

public class BarcodeComparator implements Comparator<Long>,Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final HashSet<Long> maskTable = new HashSet<Long>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			for (int i = 0; i<25; i++) {
				for (int k = 0; k < 4; k++) {
					long mask = k << i;
					add(mask);
				}
			}
		}
	};

	@Override
	public int compare(Long o1, Long o2) {
		if (o1 == o2 || maskTable.contains(o1^o2)) {
			return 0;
		}else if (o1 > o2) {
			return 1;
		}else {
			return -1;
		}
	}
}
