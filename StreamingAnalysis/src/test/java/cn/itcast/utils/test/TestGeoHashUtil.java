package cn.itcast.utils.test;

import cn.itcast.utils.GeoHashUtil;

/**
 * @Description TODO 测试geohashutil 验证：http://www.geohash.cn/
 */
public class TestGeoHashUtil {
    public static void main(String[] args) {
        // todo 根据经纬度编码
        String geohash = GeoHashUtil.encode(25.050583, 121.559322);
        System.out.println(geohash);
        // todo geohash值解码
        double[] geo = GeoHashUtil.decode("wsqqw0kf1x0h");
        System.out.println(geo[0]+" "+geo[1]);
    }
}
