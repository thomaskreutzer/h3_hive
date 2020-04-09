package com.dot.h3.example;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.hive.udf.H3ToGeoBoundaryWkt;

public class ExampleH3ToGeoBoundaryWkt {
	
	public static void main(String[] args) throws HiveException, IOException, H3InstantiationException {
		ExampleH3ToGeoBoundaryWkt t = new ExampleH3ToGeoBoundaryWkt();
		t.testConst();
	}
	
	public void testConst() throws HiveException, IOException, H3InstantiationException {
		
		// Create Instance of our class
		H3ToGeoBoundaryWkt udf = new H3ToGeoBoundaryWkt();
		
		
		
		//--------------------------------------------------------------------------------------------------------------------------------------
		//LongTest
		Long l0 = 617733122422996991L;
		LongWritable iw0 = new LongWritable(l0);
		ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.longTypeInfo, iw0);
		
		ObjectInspector[] oiArgs0 = { valueOI0 };
		
		//Test first call
		udf.initialize(oiArgs0);
		DeferredObject valueObj0 = new DeferredJavaObject(iw0);
		DeferredObject[] doArgs0 = { valueObj0 };
		System.out.println(udf.evaluate(doArgs0));
		//--------------------------------------------------------------------------------------------------------------------------------------
		
		
		//String Test
		//Text poly = new Text("");
		
		
		udf.close();
	}

}
