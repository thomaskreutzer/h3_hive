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
import com.uber.h3core.H3Core;

@Description(name = "GetH3UnidirectionalEdgesFromHexagon",
value = "_FUNC_(long index) - returns a list of long index's \n "
+ "_FUNC_(string index) - returns a list of string index ",
extended = "Returns NULL if any argument is NULL.\n"
+ "Example:\n"
+ "  > CREATE TEMPORARY FUNCTION gh3udefh AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdgesFromHexagon';\n"
+ "  > SELECT gh3udefh(599718724986994687) AS list;\n"
+ "  > +----------------------------------------------------+\n"
+ "  > |                        list                        |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | [1248237071328346111,1320294665366274047, etc.     | \n"
+ "  > +----------------------------------------------------+\n\n"

+ "Example 2:\n"
+ "  > CREATE TEMPORARY FUNCTION gh3udefh AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdgesFromHexagon';\n"
+ "  > SELECT gh3udefh('852a100bfffffff') AS list;"
+ "  > +----------------------------------------------------+\n"
+ "  > |                        list                        |\n"
+ "  > +----------------------------------------------------+\n"
+ "  > | [\"1152a100bfffffff\",\"1252a100bfffffff\", etc.   |\n"
+ "  > +----------------------------------------------------+\n\n")



public class GetH3UnidirectionalEdgesFromHexagon extends GenericUDF {
	private final ArrayList<Text> resultText = new ArrayList<Text>();
	private final ArrayList<LongWritable> resultLong = new ArrayList<LongWritable>();
	PrimitiveObjectInspector inputOI0;
	H3Core h3;
	
	public GetH3UnidirectionalEdgesFromHexagon() throws H3InstantiationException {
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
		
		//Check to ensure the value is string or long only!
		if (! ( (inputOI0 instanceof StringObjectInspector)
				|| (inputOI0 instanceof LongObjectInspector) )
			) {
			throw new UDFArgumentException("Currently only string and long can be passed into GetH3UnidirectionalEdgesFromHexagon for the first argument.\n" 
					+ "The type passed in to argument 0 is " + inputOI0.getPrimitiveCategory().name() );
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
		List<Long> indexLongArr = new ArrayList<Long>();
		List<String> indexStrArr = new ArrayList<String>();
		
		if (arg0 == null) return null;
		
		if(inputOI0.getPrimitiveCategory().name() == "STRING") {
			String indexStr = (String) inputOI0.getPrimitiveJavaObject(arg0);
			indexStrArr = h3.getH3UnidirectionalEdgesFromHexagon(indexStr);
			for(int i=0; i < indexStrArr.size(); i++) {
				resultText.add( new Text(indexStrArr.get(i)) );
			}
			return resultText;
		} else {
			Long indexL = (Long) inputOI0.getPrimitiveJavaObject(arg0);
			indexLongArr = h3.getH3UnidirectionalEdgesFromHexagon(indexL);
			for (int x = 0; x < indexLongArr.size(); x++) {
				resultLong.add( new LongWritable(indexLongArr.get(x)) );
			}
			return resultLong;
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("GetH3UnidirectionalEdgesFromHexagon", children, ",");
	}
	
}
