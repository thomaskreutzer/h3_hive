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
import org.apache.hadoop.io.IntWritable;

import com.dot.h3.exceptions.H3InstantiationException;
import com.uber.h3core.H3Core;
import com.uber.h3core.exceptions.DistanceUndefinedException;


@Description(name = "H3Distance",
value = "_FUNC_(index long, index2 long) - returns a boolean\n "
+ "_FUNC_(index string, index2 string) - returns a boolean",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION H3Distance AS 'com.dot.h3.hive.udf.H3Distance';\n"
+ "  > SELECT H3Distance(61773312317403955,631243922056054783) AS dist;\n"
+ "  > Error: Error while compiling statement: FAILED: SemanticException [Error 10014]: Line 1:7 Wrong arguments '631243922056054783': org.apache.hadoop.hive.ql.metadata.HiveException: com.uber.h3core.exceptions.DistanceUndefinedException: Distance not defined between the two indexes. (state=42000,code=10014)"


+ "Example 2 neighbors that work:\n"
+ "  > CREATE TEMPORARY FUNCTION H3Distance AS 'com.dot.h3.hive.udf.H3Distance';\n"
+ "  > SELECT H3Distance(617733122422996991,617733122423259135) AS dist;"
+ "  > +-------+\n"
+ "  > | dist  |\n"
+ "  > +-------+n"
+ "  > | 1     |\n"
+ "  > +-------+\n\n"

)

public class H3Distance extends GenericUDF {
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;
	
	public H3Distance() throws H3InstantiationException {
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
		
		//Check to ensure the value is string or long only!
		if (! ( 
				(inputOI0 instanceof LongObjectInspector) && (inputOI0 instanceof LongObjectInspector)
				|| (inputOI0 instanceof StringObjectInspector) && (inputOI0 instanceof StringObjectInspector) )
			) {
			throw new UDFArgumentException("Currently only combinations of string/string and long/long can be passed into H3Distance for the parameter.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() + "\n"
					+ "The type passed in to argument 1 is " + inputOI1.getPrimitiveCategory().name());
		}
		
		return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Index
		Object arg1 = arguments[1].get(); //Index 2
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr1 = (String) inputOI0.getPrimitiveJavaObject(arg0);
			String indexStr2 = (String) inputOI0.getPrimitiveJavaObject(arg1);
			int l1;
			try {
				l1 = h3.h3Distance(indexStr1, indexStr2);
			} catch (DistanceUndefinedException e) {
				throw new HiveException(e);
			}
			return new IntWritable( l1 );
		} else {
			Long index1L = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			Long index2L = (Long) inputOI0.getPrimitiveJavaObject(arg1);
			int l2;
			try {
				l2 = h3.h3Distance(index1L, index2L);
			} catch (DistanceUndefinedException e) {
				throw new HiveException(e);
			}
			return new IntWritable( l2 );
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("H3Distance", children, ",");
	}
	
}
