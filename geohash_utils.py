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


def hashing_array(coordinates, precision=12):
    """returns geohashed list of coordinates"""
    h = []
    for coord in coordinates:
        geohash = pgh.encode(coord[0], coord[1], precision)
        h.append(geohash)

    return h


def hashing_array_from_2180(coords_arr, precision=12):
    """returns geohashed list of coordinates in 2180"""
    h = []
    for coord in coords_arr:
        lon, lat = transform(EPSG_2180, EPSG_4326, coord[0], coord[1])
        geohash = pgh.encode(lat, lon, precision)
        h.append(geohash)

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


def get_middle_neighbours(geohash):
    centroids, precision = neighbouring_centroids(geohash)
    middle_neighbours = centroids[0:4]
    return hashing_array(middle_neighbours, precision)


def get_neighbours(geohash):
    """returns a list of neighbours (of the same precision) for given geohash"""
    centroids, precision = neighbouring_centroids(geohash)
    neighbours = hashing_array(centroids, precision)
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


def get_k_neighbours(geohash, k):
    precision = len(geohash)
    lat_centroid, lon_centroid, lat_err, lon_err = pgh.decode_exactly(geohash)
    neighbours = []
    min_corner = (lat_centroid - k * 2 * lat_err, lon_centroid - k * 2 * lon_err)
    for jumper in range(0, 2 * k + 1):
        for counter in range(0, 2 * k + 1):
            cell = [min_corner[0] + counter * 2 * lat_err, min_corner[1] + jumper * 2 * lon_err]
            neighbours.append(pgh.encode(cell[0], cell[1], precision))

    return neighbours


def coord_to_wkt_line(coordinates):
    """returns list of coordinates to a line in wkt"""
    wkt = "LINE( "
    for coord in coordinates:
        wkt += f'{str(coord[0])} {str(coord[1])}, '
    wkt = wkt[:-2]
    wkt += ')'
    return wkt


def coord_to_wkt_polygon(coordinates, interior=False):
    """returns list of coordinates to a line in wkt"""
    coordinates.append(coordinates[0])
    if not interior:
        wkt = "POLYGON( "
        for coord in coordinates:
            wkt += f'{str(coord[0])} {str(coord[1])}, '
        wkt = wkt[:-2]
        wkt += ' )'
        return wkt


def coord_to_wkt_point(coord):
    """returns list of coordinates to a point in wkt"""
    wkt = "POINT( "
    wkt += f'{str(coord[0])} {str(coord[1])}, '
    wkt = wkt[:-2]
    wkt += ')'
    return wkt


def visualize_polygon(points_list, color='red', folium_map=None):
    """visualization using folium"""
    points_list.append(points_list[0])
    lat = []
    lon = []
    for point in points_list:
        lat.append(point[0])
        lon.append(point[1])

    if folium_map is None:
        m = folium.Map(location=[sum(lat) / len(lat), sum(lon) / len(lon)], zoom_start=19, tiles='cartodbpositron')
    else:
        m = folium_map

    my_polygon = folium.Polygon(locations=points_list, weight=8, color=color)
    m.add_child(my_polygon)
    return m


def polygon_wkt_to_points(wkt):
    points_list = []
    coordinates = wkt
    for char in coordinates:
        if char.isalpha():
            coordinates = coordinates.replace(char, '')
        else:
            break
    multi = coordinates.count(')')
    if multi < 2:
        coordinates = coordinates.replace('( ', '')
        coordinates = coordinates.replace(' ) ', '')
        coordinates = coordinates.replace(' )', '')
        coordinates = coordinates.replace('(', '')
        coordinates = coordinates.replace(')', '')

    coordinates = coordinates.split(', ')
    for coord in coordinates:
        lon, lat = coord.split(' ')
        points_list.append([float(lat), float(lon)])

    return points_list[:-1]


def centroid_from_points(points_list):
    centroid = [0, 0]
    length = len(points_list)
    for point in points_list:
        centroid[0] += float(point[0])
        centroid[1] += float(point[1])

    centroid[0] /= length
    centroid[1] /= length
    return centroid
