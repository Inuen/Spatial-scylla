from cassandra.cluster import Cluster
import time
import pygeohash as pgh
from pyproj import Proj, transform
from shapely import geometry
import warnings
import geopandas as gpd


with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    EPSG_2180 = Proj(init='EPSG:2180')
    EPSG_4326 = Proj(init='EPSG:4326')


def hashing_array(coords_arr):
    h = []
    for coords in coords_arr:
        lon, lat = transform(EPSG_2180, EPSG_4326, coords[0], coords[1])
        hash = pgh.encode(lat, lon)
        h.append(hash)

    return h


def common_prefix(str1, str2):
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


def find_closest(geohash_origin, how_many=1, from_table='imgw_rain', precision=-1):
    # edit geohash_origin length to optimize time
    geohash = geohash_origin
    start = time.time()
    query = session.execute(f"select * from {from_table} where hash like '{geohash}%' allow filtering;")
    records = []
    distances = {}
    set_hashed = set()

    if len(set_hashed) > how_many:
        raise ValueError('Too generic geohash was passed. Pass more precised geohash.')

    while len(set_hashed) < how_many:
        geohash = geohash[:precision]
        query = session.execute(f"select * from {from_table} where hash like '{geohash}%' allow filtering;")
        for row in query.current_rows:
            if row[3] != geohash_origin:
                if row[3] not in set_hashed:
                    records.append(row)
                    distances[row[3]] = pgh.geohash_haversine_distance(geohash_origin, row[3])/1000
                set_hashed.add(row[3])

        if len(set_hashed) > how_many:
            for rec in records:
                distances[rec[3]] = pgh.geohash_haversine_distance(geohash_origin, rec[3])/1000
            while len(distances) > how_many:
                max_key = max(distances, key=lambda key: distances[key])
                del distances[max_key]
                set_hashed.remove(max_key)
            records.clear()

    dt = time.time() - start
    print(dt)
    return distances


def geohashes_to_polygon(array_of_hashes):
    polygon = 'POLYGON (('

    for geohash in array_of_hashes:
        lat, lon = pgh.decode(geohash)
        polygon += f'lat lon, '

    polygon = polygon[:-2]
    polygon += '))'


def geohash_boundary(geohash):

    lat, lon, lat_err, lon_err = pgh.decode_exactly(geohash)
    corner_1 = (lat - lat_err, lon - lon_err)[::-1]
    corner_2 = (lat - lat_err, lon + lon_err)[::-1]
    corner_3 = (lat + lat_err, lon + lon_err)[::-1]
    corner_4 = (lat + lat_err, lon - lon_err)[::-1]

    return geometry.Polygon([corner_1, corner_2, corner_3, corner_4, corner_1])


def polygon_to_geohashes(shapely_polygon, simple_polygon=True):
    centroid = shapely_polygon.centroid


cluster = Cluster(['localhost'], port=9082)
# cluster = Cluster(['192.168.35.237'])
session = cluster.connect('scylla')

# a = find_closest('u3teght0p')
s = pgh.decode_exactly('u3teght0p')
k = pgh.decode('u3teght0p')
print(s)
print(k)
# print(a)
