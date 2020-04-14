package com.dot.h3.example;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.hive.udf.LatLongH3ToGeoBoundaryWkt;

public class ExampleLatLongToGeoBoundaryWkt {

	public static void main(String[] args) throws HiveException, IOException, H3InstantiationException {
		ExampleLatLongToGeoBoundaryWkt t = new ExampleLatLongToGeoBoundaryWkt();
		t.testConst();
	}
	
	public void testConst() throws HiveException, IOException, H3InstantiationException {
		
		// Create Instance of our class
		LatLongH3ToGeoBoundaryWkt udf = new LatLongH3ToGeoBoundaryWkt();
		
		Double d0 = 40.86016;
		DoubleWritable dw0 = new DoubleWritable(d0);
		ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.doubleTypeInfo, dw0);
		
		Double d1 = -73.90071;
		DoubleWritable dw1 = new DoubleWritable(d1);
		ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.doubleTypeInfo, dw1);
		
		int res = 9;
		IntWritable iwRes = new IntWritable(res);
		ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, iwRes);
		
		
		ObjectInspector[] oiArgs0 = { valueOI0, valueOI1, valueOI2 };
		
		udf.initialize(oiArgs0);
		DeferredObject valueObj0 = new DeferredJavaObject(dw0);
		DeferredObject valueObj1 = new DeferredJavaObject(dw1);
		DeferredObject valueObj2 = new DeferredJavaObject(iwRes);
		DeferredObject[] doArgs0 = { valueObj0, valueObj1, valueObj2 };
		System.out.println(udf.evaluate(doArgs0));
		
		udf.close();
		
	}
}
