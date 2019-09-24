package com.geostar.geosmarter.traffic.datagenerate.util;

import com.geostar.geosmarter.traffic.datagenerate.Common;

/**
 * @author luyinxing
 * @created 2019-09-22
 */
public class StringUtils {

	public static String fulfuill(String str) {
		if (str.length() == 1) {
			return Common.ZERO + str;
		}
		return str;
	}
}
