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


@Description(name = "GetH3UnidirectionalEdge",
value = "_FUNC_(index long, index2 long) - returns a long index\n "
+ "_FUNC_(index string, index2 string) - returns a string index",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION GetH3UnidirectionalEdge AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdge';\n"
+ "  > SELECT GetH3UnidirectionalEdge(61773312317403955,631243922056054783) AS index;\n"
+ "  > Error: Error while compiling statement: FAILED: IllegalArgumentException Given indexes are not neighbors. (state=42000,code=40000)\n\n"


+ "Example 2 neighbors that work:\n"
+ "  > CREATE TEMPORARY FUNCTION GetH3UnidirectionalEdge AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdge';\n"
+ "  > SELECT GetH3UnidirectionalEdge(617733122422996991,617733122423259135) AS edge;"
+ "  > +----------------------+\n"
+ "  > |         edge         |\n"
+ "  > +----------------------+\n"
+ "  > | 1266251468764348415  |\n"
+ "  > +----------------------+\n\n"

+ "Example 3 neighbors that work from string:\n"
+ "  > CREATE TEMPORARY FUNCTION GetH3UnidirectionalEdge AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdge';\n"
+ "  > SELECT GetH3UnidirectionalEdge('892a1008003ffff','892a1008007ffff') AS edge;"
+ "  > +-------------------+\n"
+ "  > |        edge       |\n"
+ "  > +-------------------+\n"
+ "  > | 1192a1008003ffff  |\n"
+ "  > +-------------------+\n\n"
)


public class GetH3UnidirectionalEdge extends GenericUDF {
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;
	
	public GetH3UnidirectionalEdge() throws H3InstantiationException {
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
			throw new UDFArgumentException("Currently only combinations of string/string and long/long can be passed into GetH3UnidirectionalEdge for the parameter.\n" 
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
		Object arg1 = arguments[1].get(); //Index 2
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr1 = (String) inputOI0.getPrimitiveJavaObject(arg0);
			String indexStr2 = (String) inputOI0.getPrimitiveJavaObject(arg1);
			String s = h3.getH3UnidirectionalEdge(indexStr1, indexStr2);
			return new Text( s );
		} else {
			Long index1L = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			Long index2L = (Long) inputOI0.getPrimitiveJavaObject(arg1);
			Long l = h3.getH3UnidirectionalEdge(index1L, index2L);
			return new LongWritable( l );
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("GetH3UnidirectionalEdge", children, ",");
	}
	
}
