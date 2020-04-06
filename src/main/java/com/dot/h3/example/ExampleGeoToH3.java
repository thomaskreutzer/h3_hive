package com.dot.h3.example;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.hive.udf.GeoToH3;

public class ExampleGeoToH3 {

	public static void main(String[] args) throws HiveException, IOException, H3InstantiationException {
		ExampleGeoToH3 t = new ExampleGeoToH3();
		t.testConst();
	}

	public void testConst() throws HiveException, IOException, H3InstantiationException {

		// Create Instance of our class
		GeoToH3 udf = new GeoToH3();

		Double lat = 40.803415;
		Double lng = -73.949194;
		int res = 13;
		
		
		/*
		 * lat = 40.749858;
		 * lng = -73.995917;
		 * res = 13;
		 * output = 635747522321122943
		 * 
		 * lat = 40.803415;
		 * lng = -73.949194;
		 * res = 13;
		 * 635747521163695359
		 * */

		DoubleWritable dwLat = new DoubleWritable(lat);
		DoubleWritable dwLong = new DoubleWritable(lng);
		IntWritable iwRes = new IntWritable(res);

		ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.doubleTypeInfo, dwLat);
		ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.doubleTypeInfo, dwLong);
		ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, iwRes);

		ObjectInspector[] oiArgs0 = { valueOI0, valueOI1, valueOI2};
		udf.initialize(oiArgs0);

		
		DeferredObject valueObj0 = new DeferredJavaObject(dwLat);
		DeferredObject valueObj1 = new DeferredJavaObject(dwLong);
		DeferredObject valueObj2 = new DeferredJavaObject(iwRes);
		DeferredObject[] doArgs0 = { valueObj0, valueObj1, valueObj2};
		System.out.println(udf.evaluate(doArgs0));

		udf.close();
	}

}
