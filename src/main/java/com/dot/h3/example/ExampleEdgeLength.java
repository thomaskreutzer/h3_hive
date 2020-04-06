package com.dot.h3.example;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.junit.Test;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.hive.udf.EdgeLength;

public class ExampleEdgeLength {

	public static void main(String[] args) throws HiveException, IOException, H3InstantiationException {
		ExampleEdgeLength t = new ExampleEdgeLength();
		t.testEdgeLengthConst();

	}

	public void testEdgeLengthConst() throws HiveException, IOException, H3InstantiationException {

		// Create Instance of our class
		EdgeLength udf = new EdgeLength();

		Integer resolution = 12;
		String lengthUnit = "m";

		IntWritable lengthUnitWritable = new IntWritable(resolution);
		Text lengthUnitWriteable = new Text(lengthUnit);

		ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lengthUnitWritable);
		ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, lengthUnitWriteable);
		ObjectInspector[] oiArgs = { valueOI0, valueOI1 };

		udf.initialize(oiArgs);

		DeferredObject valueObj0 = new DeferredJavaObject(new IntWritable(resolution));
		DeferredObject valueObj1 = new DeferredJavaObject(new Text(lengthUnit));

		DeferredObject[] doArgs = { valueObj0, valueObj1 };

		System.out.println(udf.evaluate(doArgs));
		udf.close();
	}

}
