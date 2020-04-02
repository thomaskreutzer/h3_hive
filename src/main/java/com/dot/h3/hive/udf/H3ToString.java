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

@Description(name = "H3ToString",
value = "_FUNC_(index long) - returns H3 String",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > SELECT _FUNC_(631243922056054783);\n"
+ "  > 8c2a100acc687ff\n")


public class H3ToString extends GenericUDF {
	private final Text strOut = new Text();
	PrimitiveObjectInspector inputOI0;
	H3Core h3;
	
	public H3ToString() throws H3InstantiationException {
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
		ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		return outputOI;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Index
		
		if (arg0 == null) return null;
		
		Long index = (Long) inputOI0.getPrimitiveJavaObject(arg0);

		strOut.set( h3.h3ToString(index) );
		return strOut;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("H3ToString", children, ",");
	}
}
