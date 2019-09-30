package com.geostar.geosmarter.traffic.datagenerate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
/**
 * 产生车牌号的类，不重复
 * SSX14139U
 * */
public class VehiclePlateGenerateSG implements Serializable {

	private static final long serialVersionUID = -8006144823705880339L;

	public static Random random = new Random();

	// 主要目的就是使得产生的数字字符不在这个list里面
	// 如果在这个list里面找到，那么需要重新产生
	private static List<String> uniqueList = new ArrayList<>();

	public static String generatePlate(boolean ableDuplicatVehiclePlate) {
		String alphabeticalSeries = getAlphabeticalStr(Common.ALPHABETICAL_ARRAY, random) + getAlphabeticalStr(Common.ALPHABETICAL_ARRAY, random);
		String numbericalSeries = getNumericalSeriesStr(random, ableDuplicatVehiclePlate);
		String checksumLetter = getChecksumLetterStr(Common.ALPHABETICAL_ARRAY, random);
		//一个字母 + 两个字母 + 五位数字 + 一个字母
		String singaporeVehiclePlate = Common.ALPHABETICAL_ARRAY[18] + alphabeticalSeries + numbericalSeries + checksumLetter;
		return singaporeVehiclePlate;
	}

	/**
	 * 获取一个字母
	 * */
	private static String getAlphabeticalStr(String[] ALPHABETICAL_ARRAY, Random random) {
		String alphabeticalStr = ALPHABETICAL_ARRAY[random.nextInt(ALPHABETICAL_ARRAY.length)];
		if (!(alphabeticalStr.equals(ALPHABETICAL_ARRAY[8]) || alphabeticalStr.equals(ALPHABETICAL_ARRAY[14]))) {
			return alphabeticalStr;
		} else {
			return getAlphabeticalStr(ALPHABETICAL_ARRAY, random);
		}
	}

	/**
	 * 五位随机数字
	 * */
	private static String getNumericalSeriesStr(Random random, boolean ableDuplicatVehiclePlate) {
		String numericalStr = random.nextInt(10) + "" + random.nextInt(10) + "" + random.nextInt(10) + "" + random.nextInt(10) + "" + random.nextInt(10);
		if (ableDuplicatVehiclePlate) {
			return numericalStr;
		} else {
			if (uniqueList.contains(numericalStr)) {
				// 如果存在，则重新产生
				return getNumericalSeriesStr(random, ableDuplicatVehiclePlate);
			} else {
				uniqueList.add(numericalStr);
				return numericalStr;
			}
		}
	}

	/**
	 * 生成一个字母
	 * */
	private static String getChecksumLetterStr(String[] ALPHABETICAL_ARRAY, Random random) {
		String checksumLetter = ALPHABETICAL_ARRAY[random.nextInt(ALPHABETICAL_ARRAY.length)];
		if (!(checksumLetter.equals(Common.ALPHABETICAL_ARRAY[5])
				|| checksumLetter.equals(Common.ALPHABETICAL_ARRAY[8])
				|| checksumLetter.equals(Common.ALPHABETICAL_ARRAY[13])
				|| checksumLetter.equals(Common.ALPHABETICAL_ARRAY[14])
				|| checksumLetter.equals(Common.ALPHABETICAL_ARRAY[16])
				|| checksumLetter.equals(Common.ALPHABETICAL_ARRAY[21])
				|| checksumLetter.equals(Common.ALPHABETICAL_ARRAY[22]))) {
			return checksumLetter;
		} else {
			return getChecksumLetterStr(ALPHABETICAL_ARRAY, random);
		}
	}
}
