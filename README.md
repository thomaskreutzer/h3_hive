# h3_hive

H3 UDF's for Apache Hive



## Function List

1. EdgeLength
2. GeoToH3
3. GeoToH3Address
4. GetH3UnidirectionalEdge
5. GetH3UnidirectionalEdgesFromHexagon
6. H3Distance
7. H3IndexesAreNeighbors
8. H3SetToMultiPolygon
9. H3ToCenterChild
10. H3ToCenterChildWkt
11. H3ToChildren
12. H3ToChildrenWkt
13. H3ToGeoBoundryWkt
14. H3ToGeoWkt
15. H3ToParent
16. H3ToParentWkt
17. H3ToString
18. HexArea
19. KRing
20. KRingDistances
21. KRingToWkt
22. LatLongH3ToGeoBoundryWkt
23. NumHexagons
24. PolyfillToArrayH3Index
25. PolyfillToArrayWkt

For all of the functions listed below you execute describe.


```SQL
DESCRIBE FUNCTION <function_name>;
DESCRIBE FUNCTION EXTENDED <function_name>;
end
```

## Function Examples

**NOTE:** Temporary functions are used for these examples. Temporary functions will not work in LLAP.
You must create permanent functions in LLAP. 

### EdgeLength

```SQL
CREATE TEMPORARY FUNCTION EdgeLength AS 'com.dot.h3.hive.udf.EdgeLength';
SELECT _FUNC_(12, 'm') AS edge_meters;
+--------------+
| edge_meters  |
+--------------+
| 9.415526211  |
+--------------+
```

### GeoToH3

```SQL
CREATE TEMPORARY FUNCTION GeoToH3 AS 'com.dot.h3.hive.udf.GeoToH3';
SELECT GeoToH3(40.86016, -73.90071, 12) AS index;
+---------------------+
|        index        |
+---------------------+
| 631243922056054783  |
+---------------------+
```

### GeoToH3Address

```SQL
CREATE TEMPORARY FUNCTION GeoToH3Address AS 'com.dot.h3.hive.udf.GeoToH3Address';
SELECT GeoToH3Address(40.86016, -73.90071, 12) AS index;
+------------------+
|      index       |
+------------------+
| 892a100acc7ffff  |
+------------------+
```

### GetH3UnidirectionalEdge

**NOTE:** The indexes must be neighbors for this to work.

**BAD EXAMPLE**

```SQL
CREATE TEMPORARY FUNCTION GetH3UnidirectionalEdge AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdge';
SELECT GetH3UnidirectionalEdge(61773312317403955,631243922056054783) AS index;
Error: Error while compiling statement: FAILED: IllegalArgumentException Given indexes are not neighbors. (state=42000,code=40000)
```


**First Example:** neighbors that work

```SQL
CREATE TEMPORARY FUNCTION GetH3UnidirectionalEdge AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdge';
SELECT GetH3UnidirectionalEdge(617733122422996991,617733122423259135) AS edge;
 +----------------------+
 |         edge         |
 +----------------------+
 | 1266251468764348415  |
 +----------------------+
```

**Second Example:** neighbors that work from string:

```SQL
CREATE TEMPORARY FUNCTION GetH3UnidirectionalEdge AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdge';
SELECT GetH3UnidirectionalEdge('892a1008003ffff','892a1008007ffff') AS edge;"
 +-------------------+
 |        edge       |
 +-------------------+
 | 1192a1008003ffff  |
 +-------------------+
```


### GetH3UnidirectionalEdgesFromHexagon

**First Example:** 

```SQL
CREATE TEMPORARY FUNCTION gh3udefh AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdgesFromHexagon';
SELECT gh3udefh(599718724986994687) AS list;
+----------------------------------------------------+
|                        list                        |
+----------------------------------------------------+
| [1248237071328346111,1320294665366274047, etc.     |
+----------------------------------------------------+
```

**Second Example:** 

```SQL
CREATE TEMPORARY FUNCTION gh3udefh AS 'com.dot.h3.hive.udf.GetH3UnidirectionalEdgesFromHexagon';
SELECT gh3udefh('852a100bfffffff') AS list;"
+----------------------------------------------------+
|                        list                        |
+----------------------------------------------------+
| [\"1152a100bfffffff\",\"1252a100bfffffff\", etc.   |
+----------------------------------------------------+
```


### H3Distance

**NOTE:** The indexes must be neighbors for this to work.

```SQL
CREATE TEMPORARY FUNCTION H3Distance AS 'com.dot.h3.hive.udf.H3Distance';
SELECT H3Distance(61773312317403955,631243922056054783) AS dist;
Error: Error while compiling statement: FAILED: SemanticException [Error 10014]: Line 1:7 Wrong arguments '631243922056054783': org.apache.hadoop.hive.ql.metadata.HiveException:com.uber.h3core.exceptions.DistanceUndefinedException: Distance not defined between the two indexes. (state=42000,code=10014)
```

**Example:** neighbors that work

```SQL
CREATE TEMPORARY FUNCTION H3Distance AS 'com.dot.h3.hive.udf.H3Distance';
SELECT H3Distance(617733122422996991,617733122423259135) AS dist;
+-------+
| dist  |
+-------+
| 1     |
+-------+
```



