import pygeohash as pgh
from shapely import geometry
from shapely.ops import cascaded_union
from pyproj import Proj, transform
import warnings
import folium


with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    EPSG_2180 = Proj(init='EPSG:2180')
    EPSG_4326 = Proj(init='EPSG:4326')


def common_prefix(str1, str2):
    """returning common prefix of two geohashes"""
    prefix = ''
    for char1, char2 in zip(str1, str2):
        if char1 == char2:
            prefix += char1
        else:
            break

    return prefix


def hash_prefix(hash_arr):
    """Finding common start for array of hashes, made for relatively small polygons, lines"""
    prefix = common_prefix(hash_arr[0], hash_arr[1])
    for hash in hash_arr:
        while prefix not in hash:
            prefix = prefix[:-1]

    return prefix


def hashing_array(coords_arr):
    """returns geohashed list of coordinates"""
    h = []
    for coords in coords_arr:
        lon, lat = transform(EPSG_2180, EPSG_4326, coords[0], coords[1])
        hash = pgh.encode(lat, lon)
        h.append(hash)

    return h


def neighbouring_centroids(geohash):
    lat_centroid, lon_centroid, lat_err, lon_err = pgh.decode_exactly(geohash)
    centroid_1 = (lat_centroid + 2 * lat_err, lon_centroid)
    centroid_2 = (lat_centroid - 2 * lat_err, lon_centroid)
    centroid_3 = (lat_centroid, lon_centroid + 2 * lon_err)
    centroid_4 = (lat_centroid, lon_centroid - 2 * lon_err)
    centroid_5 = (lat_centroid + 2 * lat_err, lon_centroid + 2 * lon_err)
    centroid_6 = (lat_centroid + 2 * lat_err, lon_centroid - 2 * lon_err)
    centroid_7 = (lat_centroid - 2 * lat_err, lon_centroid + 2 * lon_err)
    centroid_8 = (lat_centroid - 2 * lat_err, lon_centroid - 2 * lon_err)

    precision = len(geohash)
    centroids = [centroid_1, centroid_2, centroid_3, centroid_4, centroid_5, centroid_6, centroid_7, centroid_8]
    return centroids, precision


def generate_neighbouring_hashes(centroids, precision=12):
    neighbours = []
    for centroid in centroids:
        geohash = pgh.encode(centroid[0], centroid[1], precision)
        neighbours.append(geohash)

    return neighbours


def get_neighbours(geohash):
    """returns a list of neighbours (of the same precision) for given geohash"""
    centroids, precision = neighbouring_centroids(geohash)
    neighbours = generate_neighbouring_hashes(centroids, precision)
    return neighbours


def get_geohash_corners(geohash):
    """returns a list of geohash's corners"""
    lat_centroid, lon_centroid, lat_err, lon_err = pgh.decode_exactly(geohash)

    corner_1 = (lat_centroid - lat_err, lon_centroid - lon_err)
    corner_2 = (lat_centroid - lat_err, lon_centroid + lon_err)
    corner_3 = (lat_centroid + lat_err, lon_centroid + lon_err)
    corner_4 = (lat_centroid + lat_err, lon_centroid - lon_err)
    corners = [corner_1, corner_2, corner_3, corner_4]
    return corners


def coord_to_wkt_line(coordinates):
    """returns list of coordinates to a line in wkt"""
    wkt = "LINE( "
    for coord in coordinates:
        wkt += f'{str(coord[0])} {str(coord[1])}, '
    wkt = wkt[:-2]
    wkt += ')'
    return wkt


def coord_to_wkt_point(coord):
    """returns list of coordinates to a point in wkt"""
    wkt = "POINT( "
    wkt += f'{str(coord[0])} {str(coord[1])}, '
    wkt = wkt[:-2]
    wkt += ')'
    return wkt


def visualize_polygon(polyline, color='red', folium_map=None):
    """visualization using folium"""
    polyline.append(polyline[0])
    lat = []
    lon = []
    for point in polyline:
        lat.append(point[0])
        lon.append(point[1])

    if folium_map is None:
        m = folium.Map(location=[sum(lat) / len(lat), sum(lon) / len(lon)], zoom_start=13, tiles='cartodbpositron')
    else:
        m = folium_map

    my_polygon = folium.Polygon(locations=polyline, weight=8, color=color)
    m.add_child(my_polygon)
    return m


coord = (53.5007, 19.495)
geohash = pgh.encode(coord[0], coord[1], precision=6)
neighbours = get_neighbours(geohash)
corners = get_geohash_corners(geohash)
print(geohash)
print(neighbours)
print(corners)
print(coord_to_wkt_point(coord))
vis = visualize_polygon(corners)
for neighbour in neighbours:
    m = visualize_polygon(get_geohash_corners(neighbour), 'blue', folium_map=vis)

m = visualize_polygon(corners, 'purple', folium_map=vis)
vis.save(" my_map2.html ")
