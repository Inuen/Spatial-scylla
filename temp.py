from cassandra.cluster import Cluster
import time
import pygeohash as pgh
from pyproj import Proj, transform


def find_closest(geohash_origin, how_many=1, from_table='imgw_rain'):
    geohash = geohash_origin
    query = session.execute(f"select * from {from_table} where hash like '{geohash}%' allow filtering;")
    records = []
    distances = {}
    set_hashed = set()
    for row in query.current_rows:
        if row[3] != geohash_origin:
            set_hashed.add(row[3])
            distances[row[3]] = pgh.geohash_haversine_distance(geohash_origin, row[3])/1000
            records.append(row)

    while len(set_hashed) < how_many:
        geohash = geohash[:-1]
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
            for key in distances:
                records.append(key)

    if len(set_hashed) > how_many:
        raise ValueError('Too generic geohash was passed. Pass more precised geohash.')

    # if len(distances) > 1:
    #     max_key = max(distances, key=lambda key: distances[key])
    #     del distances[max_key]
    return distances


cluster = Cluster(['localhost'], port=9080)
# cluster = Cluster(['192.168.35.237'])
session = cluster.connect('scylla')

start = time.time()
a = find_closest('u3t5j84u5bss')
now = time.time() - start
print(now)
print(a)
