package com.dot.h3.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.dot.h3.exceptions.H3InstantiationException;
import com.uber.h3core.H3Core;

@Description(name = "H3ToCenterChild",
value = "_FUNC_(long index, integer resolution) - returns long of Children\n "
+ "_FUNC_(string index, integer resolution) - returns string of Children",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToCenterChild AS 'com.dot.h3.hive.udf.H3ToCenterChild';\n"
+ "  > SELECT H3ToCenterChild(61773312317403955, 13) AS center_child;\n"
+ "  > +--------------------+\n"
+ "  > |    center_child    |\n"
+ "  > +--------------------+\n"
+ "  > | 61773312317403955  |\n"
+ "  > +--------------------+\n\n"


+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToCenterChild AS 'com.dot.h3.hive.udf.H3ToCenterChild';\n"
+ "  > SELECT H3ToCenterChild('db768011473333', 13) AS center_child;"
+ "  > +-----------------+\n"
+ "  > |  center_child   |\n"
+ "  > +-----------------+\n"
+ "  > | db768011473333  |\n"
+ "  > +-----------------+\n\n"

+ "NOTE: If you are in the wrong resolution you will receive a message similar to the following. \n"
+ "Error: Error while compiling statement: FAILED: IllegalArgumentException childRes 9 must be between 13 and 15, inclusive (state=42000,code=40000)"
)


public class H3ToCenterChild extends GenericUDF {
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;
	
	public H3ToCenterChild() throws H3InstantiationException {
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
		if (! ( (inputOI0 instanceof StringObjectInspector)
				|| (inputOI0 instanceof LongObjectInspector) )
			) {
			throw new UDFArgumentException("Currently only string and long can be passed into H3ToCenterChild for the first parameter.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() + "\n"
					+ "The type passed in to argument 1 is " + inputOI1.getPrimitiveCategory().name());
		}
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		} else {
			return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		}
		
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Index
		Object arg1 = arguments[1].get(); //Resolution
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		Integer res = (Integer) inputOI1.getPrimitiveJavaObject(arg1);
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
			return new Text( h3.h3ToCenterChild(indexStr,res)  );
		} else {
			Long indexL = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			return new LongWritable( h3.h3ToCenterChild(indexL,res) );
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("H3ToCenterChild", children, ",");
	}
}
