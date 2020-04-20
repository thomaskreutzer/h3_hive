package com.dot.h3.hive.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.util.Dedup;
import com.uber.h3core.H3Core;

@Description(name = "H3ToChildren",
value = "_FUNC_(long index, integer resolution) - returns long of Children\n "
+ "_FUNC_(string index, integer resolution) - returns string of Children",
extended = "Returns NULL if any argument is NULL. This returns the center point of the index. \n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToChildren AS 'com.dot.h3.hive.udf.H3ToChildren';\n"
+ "  > SELECT H3ToChildren(599718724986994687, 9) AS children;\n"
+ "  > +----------------------------------------------------+\n"
+ "  > |                      children                      |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | [617733122422996991,617733122423259135,617733122423521279, etc.. \n"
+ "  > +----------------------------------------------------+\n\n"

+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION H3ToChildren AS 'com.dot.h3.hive.udf.H3ToChildren';\n"
+ "  > SELECT H3ToChildren('852a100bfffffff', 9) AS children;"
+ "  > +----------------------------------------------------+\n"
+ "  > |                      children                      |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | [\"892a1008003ffff\",\"892a1008007ffff\", etc...   |\n"
+ "  > +----------------------------------------------------+\n\n")



public class H3ToChildren extends GenericUDF {
	private final ArrayList<Text> resultText = new ArrayList<Text>();
	private final ArrayList<LongWritable> resultLong = new ArrayList<LongWritable>();
	PrimitiveObjectInspector inputOI0;
	PrimitiveObjectInspector inputOI1;
	H3Core h3;
	Dedup dedup;
	
	public H3ToChildren() throws H3InstantiationException {
		try {
			h3 = H3Core.newInstance();
			dedup = new Dedup();
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
			throw new UDFArgumentException("Currently only string and long can be passed into H3ToChildren for the first parameter.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() + "\n"
					+ "The type passed in to argument 1 is " + inputOI1.getPrimitiveCategory().name());
		}
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		} else {
			return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
		}
		
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object arg0 = arguments[0].get(); //Index
		Object arg1 = arguments[1].get(); //Resolution
		List<Long> indexLongArr = new ArrayList<Long>();
		List<String> indexStrArr = new ArrayList<String>();
		
		if (arg0 == null) return null;
		if (arg1 == null) return null;
		Integer res = (Integer) inputOI1.getPrimitiveJavaObject(arg1);
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
			indexStrArr = h3.h3ToChildren(indexStr,res);
			for(int i=0; i < indexStrArr.size(); i++) {
				resultText.add( new Text(indexStrArr.get(i)) );
			}
			return dedup.RemoveDupsText(resultText);
		} else {
			Long indexL = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			indexLongArr = h3.h3ToChildren(indexL,res);
			for (int x = 0; x < indexLongArr.size(); x++) {
				resultLong.add( new LongWritable(indexLongArr.get(x)) );
			}
			return dedup.RemoveDupsLongWritable(resultLong);
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("H3ToChildren", children, ",");
	}
}
