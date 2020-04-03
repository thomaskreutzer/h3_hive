package com.dot.h3.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import com.dot.h3.exceptions.H3InstantiationException;
import com.uber.h3core.AreaUnit;
import com.uber.h3core.H3Core;

@Description(name = "HexArea",
value = "_FUNC_(int resolution, string areaUnit) - returns a Double\n "
+ "areaUnit can be either 'km2' or 'm2'\n",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION HexArea as 'com.dot.h3.hive.udf.HexArea'; \n"
+ "  > SELECT HexArea(9, 'km2') AS hex_area; \n"
+ "  > +------------+\n"
+ "  > |  hex_area  |\n"
+ "  > +------------+\n"
+ "  > | 0.1053325  |\n"
+ "  > +------------+\n\n"


+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION HexArea as 'com.dot.h3.hive.udf.HexArea'; \n"
+ "  > SELECT HexArea(9, 'm2') AS hex_area; \n"
+ "  > +-----------+\n"
+ "  > | hex_area  |\n"
+ "  > +-----------+\n"
+ "  > | 105332.5  |\n"
+ "  > +-----------+\n\n"
)



public class HexArea extends GenericUDF {
	private final DoubleWritable DoubleReturn = new DoubleWritable();
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;
	
	public HexArea() throws H3InstantiationException {
		try {
			h3 = H3Core.newInstance();
		} catch (IOException e) {
			throw new H3InstantiationException("Could not instantiate the H3 core library");
		}
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		checkArgsSize(arguments, 2, 2);
		checkArgPrimitive(arguments, 0);
		
		inputOI0 = (PrimitiveObjectInspector)arguments[0];
		inputOI1 = (PrimitiveObjectInspector)arguments[1];
		
		//Check to ensure the value is string or long only!
		if (! ( (inputOI0 instanceof IntObjectInspector)
				&& (inputOI1 instanceof StringObjectInspector) )
			) {
			throw new UDFArgumentException("Currently only int and string can be passed into HexArea for the respective parameters resolution and areaUnit.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() + "\n"
					+ "The type passed in to argument 1 is " + inputOI1.getPrimitiveCategory().name());
		}
		
		return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
	}
	
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		AreaUnit areaUnit;
		areaUnit = AreaUnit.km2;
		Object arg0 = arguments[0].get(); //resolution
		Object arg1 = arguments[1].get(); //areaUnit
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		
		//Get the resolution variable
		Integer res = (Integer) inputOI0.getPrimitiveJavaObject(arg0);
		String auStr = (String) inputOI1.getPrimitiveJavaObject(arg1);
		
		if(!auStr.equals("km2")  && !auStr.equals("m2")) {
			throw new HiveException("The parameter areaUnit has the value '" + auStr + "' and must be either km2 or m2");
		}
		
		if(auStr.equals("km2")) {
			areaUnit = AreaUnit.km2;
		} else {
			areaUnit = AreaUnit.m2;
		}
		DoubleReturn.set( h3.hexArea(res, areaUnit) );
		return DoubleReturn;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("HexArea", children, ",");
	}
	
}
