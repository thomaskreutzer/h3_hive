package com.dot.h3.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

import com.dot.h3.exceptions.H3InstantiationException;
import com.uber.h3core.H3Core;

public class NumHexagons extends GenericUDF {
	private final LongWritable longReturn = new LongWritable();
	PrimitiveObjectInspector inputOI0;
	H3Core h3;

	public NumHexagons() throws H3InstantiationException {
		try {
			h3 = H3Core.newInstance();
		} catch (IOException e) {
			throw new H3InstantiationException("Could not instantiate the H3 core library");
		}
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		checkArgsSize(arguments, 1, 1);
		checkArgPrimitive(arguments, 0);
		
		inputOI0 = (PrimitiveObjectInspector)arguments[0];
		ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		return outputOI;
	}
	
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //res
		
		if (arg0 == null) return null;
		
		//Get the resolution variable
		Integer res = (Integer) inputOI0.getPrimitiveJavaObject(arg0);
		longReturn.set( h3.numHexagons(res) );
		
		return longReturn;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("NumHexagons", children, ",");
	}
	

}
