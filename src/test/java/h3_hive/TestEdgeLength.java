package h3_hive;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.dot.h3.exceptions.H3InstantiationException;
import com.dot.h3.hive.udf.EdgeLength;

public class TestEdgeLength {

	@Test
	public void shouldEqual() throws HiveException, H3InstantiationException, IOException {
		// given
		Integer resolution = 12;
		String lengthUnit = "m";

		IntWritable lengthUnitWritable = new IntWritable(resolution);
		Text lengthUnitWriteable = new Text(lengthUnit);

		ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lengthUnitWritable);
		ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, lengthUnitWriteable);
		ObjectInspector[] oiArgs = { valueOI0, valueOI1 };

		

		DeferredObject valueObj0 = new DeferredJavaObject(new IntWritable(resolution));
		DeferredObject valueObj1 = new DeferredJavaObject(new Text(lengthUnit));

		DeferredObject[] doArgs = { valueObj0, valueObj1 };
		
		// when & then
		EdgeLength udf = new EdgeLength();
		udf.initialize(oiArgs);
		
		
		DoubleWritable expected = new DoubleWritable(9.415526211);
		Assert.assertEquals(expected, udf.evaluate(doArgs));
		
		udf.close();
	}

}
