from cassandra.cluster import Cluster
import time
import pygeohash as pgh
from shapely import geometry


def find_closest(session, geohash_origin, how_many=1, from_table='imgw_rain', precision=-1):
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
    return polygon


def geohash_boundary(geohash):

    lat, lon, lat_err, lon_err = pgh.decode_exactly(geohash)
    corner_1 = (lat - lat_err, lon - lon_err)[::-1]
    corner_2 = (lat - lat_err, lon + lon_err)[::-1]
    corner_3 = (lat + lat_err, lon + lon_err)[::-1]
    corner_4 = (lat + lat_err, lon - lon_err)[::-1]

    return geometry.Polygon([corner_1, corner_2, corner_3, corner_4, corner_1])
