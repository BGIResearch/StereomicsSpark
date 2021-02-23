package utils;

import utils.Utils;

public class TestMain {
	public static void main(String[] args) {
		String barcode = "AAAAAAATGC";
		long barcodeInt = Utils.encode(barcode);
		String decodeBarcode = Utils.decode(barcodeInt, 10);
		System.out.println(barcode + "\t" + barcodeInt + "\t" + decodeBarcode);
	}
}
