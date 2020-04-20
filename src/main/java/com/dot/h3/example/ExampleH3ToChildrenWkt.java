package com.dot.h3.example;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.hive.udf.H3ToChildrenWkt;

public class ExampleH3ToChildrenWkt {

	public static void main(String[] args) throws HiveException, IOException, H3InstantiationException {
		ExampleH3ToChildrenWkt t = new ExampleH3ToChildrenWkt();
		t.testConst();
	}
	
public void testConst() throws HiveException, IOException, H3InstantiationException {
		
		// Create Instance of our class
		H3ToChildrenWkt udf = new H3ToChildrenWkt();
		
		Long l0 = 599718724986994687L;
		int res = 6;
		
		LongWritable lw0 = new LongWritable(l0);
		IntWritable iwRes = new IntWritable(res);
		
		ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.longTypeInfo, lw0);
		ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, iwRes);
		
		ObjectInspector[] oiArgs0 = { valueOI0 , valueOI1 };
		
		udf.initialize(oiArgs0);
		DeferredObject valueObj0 = new DeferredJavaObject(lw0);
		DeferredObject valueObj1 = new DeferredJavaObject(iwRes);
		DeferredObject[] doArgs0 = { valueObj0 , valueObj1 };
		System.out.println(udf.evaluate(doArgs0));
		
		udf.close();
	}

}
