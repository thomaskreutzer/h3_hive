package com.dot.h3.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.dot.h3.util.WKT;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;

public class H3Example {
	WKT wkt;
	
	public H3Example() {
		wkt = new WKT();
	}

	public static void main(String[] args) {
		H3Example ex = new H3Example();
		ex.test();
	}

	public void test() {
		try {
			H3Core h3 = H3Core.newInstance();

			double lat = 40.86016;
			double lng = -73.90071;
			int res = 9;

			String hexAddr = h3.geoToH3Address(lat, lng, res);

			long h3index = h3.geoToH3(lat, lng, res);

			List<GeoCoord> cords = h3.h3ToGeoBoundary(h3index);

			System.out.println(hexAddr);
			System.out.print("H3 Index: " + h3index + "\n");

			for (int i = 0; i < cords.size(); i++) {
				System.out.println( cords.get(i) );
			}
			
			
			String wktPolygon = wkt.geoCoordToPolygonWkt(cords);
			System.out.println( wktPolygon );
			
			
			GeoCoord cords2 = h3.h3ToGeo(h3index);
			
			
			System.out.println("Lat: " +cords2.lat);
			System.out.println("Long: " +cords2.lng);
			
			
			//NOT USED AT THE MOMENT
			/*
			String geo_json = "{\"type\":\"Polygon\",\"coordinates\":[[[-1.7358398437499998,52.24630137198303],"
					+ "[-1.8923950195312498,52.05249047600099],[-1.56829833984375,51.891749018068246],"
					+ "[-1.27716064453125,51.91208502557545],[-1.19476318359375,52.032218104145294],"
					+ "[-1.24420166015625,52.19413974159753],[-1.5902709960937498,52.24125614966341],"
					+ "[-1.7358398437499998,52.24630137198303]],[[-1.58203125,52.12590076522272],"
					+ "[-1.476287841796875,52.12590076522272],[-1.46392822265625,52.075285904832334],"
					+ "[-1.58203125,52.06937709602395],[-1.58203125,52.12590076522272]],"
					+ "[[-1.4556884765625,52.01531743663362],[-1.483154296875,51.97642166216334],"
					+ "[-1.3677978515625,51.96626938051444],[-1.3568115234375,52.0102459910103],"
					+ "[-1.4556884765625,52.01531743663362]]]}";*/
			
			
			String multipolygon = "MULTIPOLYGON(((-71.1031880899493 42.3152774590236,\n" + 
					"-71.1031627617667 42.3152960829043,-71.102923838298 42.3149156848307,\n" + 
					"-71.1023097974109 42.3151969047397,-71.1019285062273 42.3147384934248,\n" + 
					"-71.102505233663 42.3144722937587,-71.10277487471 42.3141658254797,-71.103113945163 42.3142739188902,-71.10324876416 42.31402489987,-71.1033002961013 42.3140393340215,-71.1033488797549 42.3139495090772,-71.103396240451 42.3138632439557,-71.1041521907712 42.3141153348029,-71.1041411411543 42.3141545014533,-71.1041287795912 42.3142114839058,-71.1041188134329 42.3142693656241,-71.1041112482575 42.3143272556118,-71.1041072845732 42.3143851580048,-71.1041057218871 42.3144430686681,-71.1041065602059 42.3145009876017,-71.1041097995362 42.3145589148055,-71.1041166403905 42.3146168544148,-71.1041258822717 42.3146748022936,-71.1041375307579 42.3147318674446,-71.1041492906949 42.3147711126569,-71.1041598612795 42.314808571739,-71.1042515013869 42.3151287620809,-71.1041173835118 42.3150739481917,-71.1040809891419 42.3151344119048,-71.1040438678912 42.3151191367447,-71.1040194562988 42.3151832057859,-71.1038734225584 42.3151140942995,-71.1038446938243 42.3151006300338,-71.1038315271889 42.315094347535,-71.1037393329282 42.315054824985,-71.1035447555574 42.3152608696313,-71.1033436658644 42.3151648370544,-71.1032580383161 42.3152269126061,-71.103223066939 42.3152517403219,-71.1031880899493 42.3152774590236)),((-71.1043632495873 42.315113108546,-71.1043583974082 42.3151211109857,\n" + 
					"-71.1043443253471 42.3150676015829,-71.1043850704575 42.3150793250568,-71.1043632495873 42.315113108546)))";
			
			String poly = "POLYGON((-71.23094863399959 42.35171702149799,-71.20507841890782 42.39384377360396,-71.18534241583312 42.40583588152941,-71.13489748711537 42.40374196572458,-71.12786523200806 42.3537116038451,-71.23094863399959 42.35171702149799))";
			
			//Polyfill Testing
			List<Long> ll = h3.polyfill(cords, null, 10);
			List<String> wktList = new ArrayList<String>();
			for (int i = 0; i < ll.size(); i++) {
				wktList.add(wkt.geoCoordToPolygonWkt(h3.h3ToGeoBoundary(i)));
			}
			System.out.println("WKT List: " + wktList.toString());
			
			//Converting WKT to GeoCoords from H3
			System.out.println("WKT To GeoCoords\n");
			List<GeoCoord> newGeoCoordsWkt = wkt.wktPolygonToGeoCoord(wktPolygon);
			System.out.println(newGeoCoordsWkt.toString());
			
			
			
			//Convert mutipolygon WKT to GeoCoord list of lists.
			System.out.println("mutipolygon WKT: " + wkt.wktMultiPolygonToGeoCoord(multipolygon).toString() );
			System.out.println("polygon WKT " + wkt.wktMultiPolygonToGeoCoord(wktPolygon).toString() );
			
			
			List<GeoCoord> cords3  = wkt.wktPolygonToGeoCoord(poly);
			System.out.println("Cords 3:" + cords3.size());
			for (int i = 0; i < cords3.size(); i++) {
				System.out.println(cords3.get(i));
			}
			
			List<Long> indexArr = h3.polyfill(wkt.wktPolygonToGeoCoord(poly), null, 12);
			System.out.println("Index Array: " + indexArr.toString());
			
			
			
			Long numHexagons = h3.numHexagons(9);
			System.out.println("Number of Hexagons at resolution 9: " + numHexagons);
			
			
			List<String> kring = h3.kRing("892a100acc7ffffb", 1);
			
			Long kringSearchL = 631243922056054783L;
			List<Long> kringL = h3.kRing(kringSearchL, 1);
			System.out.println("Kring:\n" + kring.toString());
			System.out.println("Kring from Long:\n" + kringL.toString());
			
			
			Long KRingDistanceIndex = 631243922056054783L;
			List<List<Long>> krd = h3.kRingDistances(KRingDistanceIndex, 3);
			System.out.println("KRingDistance Output: ");
			for (int i = 0; i < krd.size(); i++) {
				System.out.println("Output " + i + ": " + krd.get(i));
			}
			
			String KringDistanxtStr = wkt.KringLongIndexToMultiPolygon(krd, h3);
			System.out.println(KringDistanxtStr);
			// SELECT H3ToGeoWkt(631243922056054783) AS wkt;
			
			//SELECT H3ToString(631243922056054783);
			String KRingDistanceIndex2 = "8c2a100acc687ff";
			List<List<String>> krd2 = h3.kRingDistances(KRingDistanceIndex2, 3);
			KringDistanxtStr = wkt.KringStringndexToMultiPolygon(krd2, h3);
			System.out.println(KringDistanxtStr);
			
		} catch (IOException e) {
				
		}
		
		
		System.out.println("\n\n");
		//Test building a lat/long string WKT
		double[] d = new double[4];
		d[0] = 14.909;
		d[1] = 17.8989;
		d[2] = 17.8989;
		d[3] = 17.8989;
		
		System.out.println(wkt.polygonWkt(d));
		
	}
}



//GET THE BIN IDS
//get bin id's for the geometry within the bounding box. '
//Output the bin id, bounding box, aggregated
//Validate function calls

//h3ToGeoBoundary make this return wkt in Hive UDF


//Need two functions
// latLonH3ToGeoBoundryWkt
// h3ToGeoBoundaryWkt

