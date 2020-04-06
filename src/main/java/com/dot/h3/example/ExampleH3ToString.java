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
import com.dot.h3.hive.udf.H3ToString;

public class ExampleH3ToString {
	
	public static void main(String[] args) throws HiveException, IOException, H3InstantiationException {
		ExampleH3ToString t = new ExampleH3ToString();
		t.testConst();
	}
	
	public void testConst() throws HiveException, IOException, H3InstantiationException {

		// Create Instance of our class
		H3ToString udf = new H3ToString();
		
		Long l0 = 617733122422996991L;
		Long l1 = 617733122423259135L;
		
		
		LongWritable iw0 = new LongWritable(l0);
		LongWritable iw1 = new LongWritable(l1);
		
		ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.longTypeInfo, iw0);
		ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.longTypeInfo, iw1);
		
		
		ObjectInspector[] oiArgs0 = { valueOI0 };
		ObjectInspector[] oiArgs1 = { valueOI1 };

		//Test first call
		udf.initialize(oiArgs0);
		DeferredObject valueObj0 = new DeferredJavaObject(new LongWritable(l0));
		DeferredObject[] doArgs0 = { valueObj0 };
		System.out.println(udf.evaluate(doArgs0));
		
		
		//Test Second call
		udf.initialize(oiArgs1);
		DeferredObject valueObj1 = new DeferredJavaObject(new LongWritable(l1));
		DeferredObject[] doArgs1 = { valueObj1 };
		System.out.println(udf.evaluate(doArgs1));
		
		udf.close();
	}
	
}
