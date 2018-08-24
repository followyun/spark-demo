package my.spark.sql;

import com.esri.core.geometry.*;
import org.json.JSONException;

/**
 * geometry库的使用
 */
public class GeometryFromGeoJsonTest {
//根据geojson json格式数据获取到地理信息(多边形)
    public static Polygon getPolygonFromGeoJson() throws JSONException {
        String geoJsonStr = "{\"type\":\"Polygon\","
                + "\"coordinates\":[[[[0.0,0.0],[-0.5,0.5],[0.0,1.0],[0.5,1.0],[1.0,0.5],[0.5,0.0],[0.0,0.0]],"
                + "[[0.5,0.2],[0.6,0.5],[0.2,0.9],[-0.2,0.5],[0.1,0.2],[0.2,0.3],[0.5,0.2]]],"
                + "[[[0.1,0.7],[0.3,0.7],[0.3,0.4],[0.1,0.4],[0.1,0.7]]]],"
                + "\"crs\":\"EPSG:4326\"}"; //EPSG:指坐标类型，这里是全球坐标
        MapGeometry mapGeom =  GeometryEngine.geometryFromGeoJson(geoJsonStr, GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Polygon);
        return (Polygon) mapGeom.getGeometry();
    }

    //点
    static Point createPointFromGeoJson() throws JSONException {

        String geoJsonString = "{\"type\":\"Point\",\"coordinates\":[-106.4453583,39.11775],\"crs\":\"EPSG:4326\"}";

        MapGeometry mapGeom =  GeometryEngine.geometryFromGeoJson(geoJsonString, GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Point);

        return (Point)mapGeom.getGeometry();
    }

    public static void main(String[] args) {
        try {
           Point polygon = createPointFromGeoJson();
            System.out.println(polygon.toString());
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
