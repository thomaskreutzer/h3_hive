package com.dot.h3.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.dot.h3.exceptions.H3InstantiationException;
import com.uber.h3core.H3Core;

import org.apache.hadoop.io.Text;


@Description(name = "GeoToH3Address",
value = "_FUNC_(double lat, double long, integer resolution) - returns a String of the H3 index",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION GeoToH3Address AS 'com.dot.h3.hive.udf.GeoToH3Address';"
+ "  > SELECT GeoToH3Address(40.86016, -73.90071, 12);\n"
+ "  > 892a100acc7ffff\n"
+ " The resolution can be between 0 and 15, 15 is the most granular")


public class GeoToH3Address extends GenericUDF {
	private final Text strOut = new Text();
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	PrimitiveObjectInspector inputOI2;
	H3Core h3;
	
	public GeoToH3Address() throws H3InstantiationException {
		try {
			h3 = H3Core.newInstance();
		} catch (IOException e) {
			throw new H3InstantiationException("Could not instantiate the H3 core library");
		}
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		checkArgsSize(arguments, 3, 3);
		checkArgPrimitive(arguments, 0);
		
		inputOI0 = (PrimitiveObjectInspector)arguments[0];
		inputOI1 = (PrimitiveObjectInspector)arguments[1];
		inputOI2 = (PrimitiveObjectInspector)arguments[2];
		
		ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		return outputOI;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Latitude
		Object arg1 = arguments[1].get(); //Longitude
		Object arg2 = arguments[2].get(); //Resolution
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		if (arg2 == null) return null;
		
		Double lat = (Double) inputOI0.getPrimitiveJavaObject(arg0);
		Double lon = (Double) inputOI1.getPrimitiveJavaObject(arg1);
		Integer res = (Integer) inputOI2.getPrimitiveJavaObject(arg2);
		strOut.set( h3.geoToH3Address(lat, lon, res) );
		return strOut;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("GeoToH3Address", children, ",");
	}
}
