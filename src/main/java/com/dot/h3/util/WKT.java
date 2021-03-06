package com.dot.h3.util;

import java.util.ArrayList;
import java.util.List;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;

public class WKT {
	Dedup dedup;
	
	public WKT() {
		dedup=new Dedup();
		
	}
	
	public String geoCoordToPolygonWkt(List<GeoCoord> gc){
		StringBuilder builder=new StringBuilder();
		String repeatVal = "";
		builder.append("POLYGON((");
		for (int i = 0; i < gc.size(); i++) {
			String lngLat =  gc.get(i).lng + " " + gc.get(i).lat;
			if(i>0){
				builder.append(",");
			}
			if(i==0){
				repeatVal = lngLat;
			}
			builder.append( lngLat );
			//If this is the last in the inner loop and not the first loop append the final closer...
			if(i==gc.size()-1 && i>0) {
				builder.append("," + repeatVal);
			}
		}
		builder.append("))");
		return builder.toString();
	}
	
	public String polygonWkt(double[] latlon){
		if(latlon.length%2 != 0)
			throw new IllegalArgumentException("latlon length is not even");
			StringBuilder builder=new StringBuilder();
			builder.append("POLYGON((");
			for(int i=0;i<latlon.length;i+=2){
			if(i>0){
				builder.append(",");
			}
			builder.append(latlon[i]+" "+latlon[i+1]);
		}
		builder.append("))");
		return builder.toString();
	}
	
	public String geoCoordToPointWkt(GeoCoord gc) {
		StringBuilder builder=new StringBuilder();
		builder.append("POINT(");
		builder.append( gc.lng + " " + gc.lat );
		builder.append(")");
		return builder.toString();
	}
	
	public List<GeoCoord> wktPolygonToGeoCoord(String wkt) {
		List<GeoCoord> lgc = new ArrayList<GeoCoord>();
		//This has to clean up any number of spaces after the comma as seen in the last replaceAll
		wkt = wkt.replace("POLYGON((", "").replace("POLYGON ((", "").replace("POINT(", "").replace("POINT (", "").replace("))", "").replace(")", "").replaceAll(",\\s*", ",");
		String[] splt = wkt.split(",");
		for (int i = 0; i < splt.length; i++) {
			String[] longLat = splt[i].split(" "); //Split on the space
			String lng = longLat[0];
			String lat = longLat[1];
			//NOTE: WKT is in the order LNG/LAT  Uber H3 GeoCoord is in the Order LAT/LNG
			lgc.add( new GeoCoord( Double.parseDouble( lat.trim() ), Double.parseDouble( lng.trim() ) ) );
		}
		return lgc;
	}
	
	
	public List<List<GeoCoord>> wktMultiPolygonToGeoCoord(String wkt) {
		List<List<GeoCoord>> lgc = new ArrayList<List<GeoCoord>>();
		List<String> polygons;
		
		String multipolygonStripped= "";
		if(wkt.contains("MULTIPOLYGON")) {
			multipolygonStripped = wkt.replace("MULTIPOLYGON(", "").replace("MULTIPOLYGON (", "").replace(")))", "").replace("\n", "").replace("\r", "").replaceAll(",\\s*", ",");
			polygons = cleanPolygonsArr( multipolygonStripped.split("\\),\\(") );
			//Loop each of the polygons returned as the List<String>
			for (int i = 0; i < polygons.size(); i++) {
				List<GeoCoord> gc = new ArrayList<GeoCoord>();
				String[] longLatArr = polygons.get(i).split(",");
				for(int x = 0; x < longLatArr.length; x++) {
					String[] longLat = longLatArr[x].split(" "); //Split on the space
					String lng = longLat[0];
					String lat = longLat[1];
					//NOTE: WKT is in the order LNG/LAT  Uber H3 GeoCoord is in the Order LAT/LNG
					gc.add( new GeoCoord( Double.parseDouble( lat.trim() ), Double.parseDouble( lng.trim() ) ) ); 
				}
				//Add the list of coordinates to the list
				lgc.add(gc);
			}
		} else {
			//Only Polygon
			lgc.add(wktPolygonToGeoCoord(wkt));
		}
		return lgc;
	}
	
	public String KringLongIndexToMultiPolygon(List<List<Long>> l, H3Core h3) {
		StringBuilder wkt = new StringBuilder();
		String repeatVal = "";
		wkt.append("MULTIPOLYGON(");
		for (int i = 0; i < l.size(); i++) {
			if(i>0){
				wkt.append(",");
			}
			//For each set of items we must append a set of brackets
			wkt.append("((");
			
			List<Long> indicies = l.get(i);
			for (int x = 0; x < indicies.size(); x++) {
				if(x>0){
					wkt.append(",");
				}
				GeoCoord gc = h3.h3ToGeo( indicies.get(x) );
				String lngLat = gc.lng + " " + gc.lat;
				wkt.append(lngLat);
				if(i>0 && x==0){
					repeatVal = lngLat;
				}
				
				//If this is the last in the inner loop and not the first loop append the final closer...
				if(x==indicies.size()-1 && i>0) {
					wkt.append("," + repeatVal);
				}
			}
			//Close out the brackets
			wkt.append("))");
		}
		//Close out MULTIPOLYGON
		wkt.append(")");
		return wkt.toString();
	}
	
	public String KringStringndexToMultiPolygon(List<List<String>> l, H3Core h3) {
		StringBuilder wkt = new StringBuilder();
		String repeatVal = "";
		wkt.append("MULTIPOLYGON(");
		for (int i = 0; i < l.size(); i++) {
			if(i>0){
				wkt.append(",");
			}
			//For each set of items we must append a set of brackets
			wkt.append("((");
			
			List<String> indicies = l.get(i);
			for (int x = 0; x < indicies.size(); x++) {
				if(x>0){
					wkt.append(",");
				}
				GeoCoord gc = h3.h3ToGeo( indicies.get(x) );
				String lngLat = gc.lng + " " + gc.lat;
				wkt.append(lngLat);
				if(i>0 && x==0){
					repeatVal = lngLat;
				}
				
				//If this is the last in the inner loop and not the first loop append the final closer...
				if(x==indicies.size()-1 && i>0) {
					wkt.append("," + repeatVal);
				}
			}
			//Close out the brackets
			wkt.append("))");
		}
		//Close out MULTIPOLYGON
		wkt.append(")");
		return wkt.toString();
	}
	
	public ArrayList<String> h3SetToMultiPolygonWktArray(List<List<List<GeoCoord>>> d){
		ArrayList<String> r = new ArrayList<>();
		for (int i = 0; i < d.size(); i++) {
			for (int x = 0; x < d.get(i).size(); x++) {
				String poly = geoCoordToPolygonWkt(d.get(i).get(x));
				r.add(poly);
			}
		}
		return dedup.RemoveDupsStr(r);
	}
	
	private List<String> cleanPolygonsArr(String[] polygon) {
		List<String> r = new ArrayList<String>();
		for (int i = 0; i < polygon.length; i++) {
			r.add (polygon[i].replaceAll("\\)", "").replaceAll("\\(", "") );
		}
		return r;
	}
	

}
