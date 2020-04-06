package com.dot.h3.example;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.hive.udf.H3SetToMultiPolygon;

public class ExampleH3SetToMultiPolygon {

	public static void main(String[] args) throws HiveException, IOException, H3InstantiationException {
		ExampleH3SetToMultiPolygon t = new ExampleH3SetToMultiPolygon();
		t.testTextConst();
	}

	public void testTextConst() throws HiveException, IOException, H3InstantiationException {

		H3SetToMultiPolygon udf = new H3SetToMultiPolygon();

		//TEST CASE 1: -- STRING ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
		ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector)) };
		udf.initialize(inputOIs);
		
		Object arr = asList(new Text("8d2a100d2c9067f"),new Text("8d2a1008dcc14ff"));
		GenericUDF.DeferredJavaObject[] args = { new GenericUDF.DeferredJavaObject(arr) };
		@SuppressWarnings("unchecked")
		List<Object> result = (List<Object>) udf.evaluate(args);
		
		for (int i = 0; i < result.size(); i++) {
			System.out.println(result.get(i).toString());
		}
		
		System.out.println("\n\n");
		udf.close();
		
		
		H3SetToMultiPolygon udf2 = new H3SetToMultiPolygon();
		//TEST CASE 2: -- LONG ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
		ObjectInspector[] inputOI2s = { ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector)) };
		udf2.initialize(inputOI2s);
		Object arr2 = asList(new LongWritable(635747522321122943L),new LongWritable(635747521163695359L));
		GenericUDF.DeferredJavaObject[] args2 = { new GenericUDF.DeferredJavaObject(arr2) };
		@SuppressWarnings("unchecked")
		List<Object> result2 = (List<Object>) udf2.evaluate(args2);
		
		for (int x = 0; x < result2.size(); x++) {
			System.out.println(result2.get(x).toString());
		}
		udf2.close();
	}
	
}
