package com.dot.h3.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import com.dot.h3.exceptions.H3InstantiationException;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;

@Description(name = "GeoToH3",
value = "_FUNC_(resolution integer, lengthUnit string) - returns a double of the unit",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > SELECT _FUNC_(12, 'm') AS edge_meters;\n"
+ "  > +--------------+\n"
+ "  > | edge_meters  |\n"
+ "  > +--------------+\n"
+ "  > | 9.415526211  |\n"
+ "  > +--------------+\n"
+ " The resolution can be between 0 and 15, 15 is the most granular\n"
+ "The length unit is either m or km for meteres and kilometers")



public class EdgeLength extends GenericUDF {
	private final DoubleWritable doubleReturn = new DoubleWritable();
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;

	public EdgeLength() throws H3InstantiationException {
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
		ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
		return outputOI;
	}
	
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		LengthUnit unit;
		unit = LengthUnit.m;
		Object arg0 = arguments[0].get(); //res
		Object arg1 = arguments[1].get(); //LengthUnit either m or km
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		
		//Get the resolution variable
		Integer res = (Integer) inputOI0.getPrimitiveJavaObject(arg0);
		String lu = (String) inputOI1.getPrimitiveJavaObject(arg1);
		
		if (lu.equals("m")) {
			unit = LengthUnit.m;
		} else if (lu.equals("km")) {
			unit = LengthUnit.km;
		} else {
			throw new HiveException("The argument for Length Unit is invalid");
		}

		;
		
		doubleReturn.set(h3.edgeLength(res, unit));
		return doubleReturn;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("EdgeLength", children, ",");
	}
	

}
