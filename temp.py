from cassandra.cluster import Cluster
import time
import pygeohash as pgh
from pyproj import Proj, transform


def find_closest(geohash_origin, how_many=2, from_table='imgw_rain'):
    geohash = geohash_origin
    query = session.execute(f"select * from {from_table} where hash like '{geohash}%' allow filtering;")
    records = []
    distances = {}
    set_hashed = set()
    for row in query.current_rows:
        set_hashed.add(row[3])
        distances[row[3]] = pgh.geohash_haversine_distance(geohash_origin, row[3])
        records.append(row)

    while (len(set_hashed) < how_many and geohash_origin in set_hashed) or ((geohash_origin not in set_hashed) and (len(set_hashed) < 1)):
        geohash = geohash[:-1]
        query = session.execute(f"select * from {from_table} where hash like '{geohash}%' allow filtering;")
        for row in query.current_rows:
            if row[3] not in set_hashed:
                records.append(row)
                distances[row[3]] = pgh.geohash_haversine_distance(geohash_origin, row[3])
            set_hashed.add(row[3])

        if len(set_hashed) > how_many:#####
            for rec in records:
                distances[rec[3]] = pgh.geohash_haversine_distance(geohash_origin, rec[3])
            while len(distances) > how_many and len(distances) > 2:
                max_key = max(distances, key=lambda key: distances[key])
                del distances[max_key]
                set_hashed.remove(max_key)
            records.clear()
            for key in distances:
                records.append(key)

    if len(set_hashed) > how_many:
        raise ValueError('Too generic geohash was passed. Pass more precised geohash.')

    if geohash_origin in distances.keys():
        del distances[geohash_origin]
    elif len(distances) > 1:
        max_key = max(distances, key=lambda key: distances[key])
        del distances[max_key]
    return distances


cluster = Cluster(['localhost'], port=9080)
# cluster = Cluster(['192.168.35.237'])
session = cluster.connect('scylla')
s1 = pgh.encode(52.232855, 20.9211132)
s2 = pgh.encode(54.5038046, 18.39344)
s3 = pgh.encode(53.013372, 18.5315095)

start = time.time()
a = find_closest(s3)
now = time.time() - start
print(now)
print(a)




v = [s1, s2, s3]