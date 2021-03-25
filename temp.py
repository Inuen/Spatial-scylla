from cassandra.cluster import Cluster
import time
import pygeohash as pgh
from pyproj import Proj, transform


def find_closest(geohash_origin, how_many=2):
    geohash = geohash_origin
    query = session.execute(f"select * from imgw_rain where hash like '{geohash}%' allow filtering;")
    records = []
    distances = {}
    set_hashed = []
    for row in query.current_rows:
        set_hashed.append(row[3])
        records.append(row)

    set_hashed = set(set_hashed)
    while len(set_hashed) < how_many:
        geohash = geohash[:-1]
        query = session.execute(f"select * from imgw_rain where hash like '{geohash}%' allow filtering;")
        for row in query.current_rows:
            if row[3] not in set_hashed:
                records.append(row)
            set_hashed.add(row[3])

        if len(set_hashed) > how_many:
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
        for rec in query:
            records.append(rec)
            distances[rec[3]] = pgh.geohash_haversine_distance(geohash, rec[3])

        while len(distances) > how_many and len(distances) > 2:
            max_key = max(distances, key=lambda key: distances[key])
            del distances[max_key]

        for rec in records:
            if rec[3] not in distances.keys():
                records.remove(rec)

    return distances


cluster = Cluster(['localhost'], port=9080)
# cluster = Cluster(['192.168.35.237'])
session = cluster.connect('scylla')
# query = session.execute("select * from new_rain3 where id = 9042;")
# row = query.one()
# hash = row[3]
start = time.time() #u3x6518j47bv
# a = find_closest('u3mdptw')
a = find_closest('u3x6518j47b')
for i in a:
    print(i, a[i])
# query = session.execute(f"select * from new_rain3 where hash like 'u3mdptw%' allow filtering;")
now = time.time() - start
print(now)

s1 = pgh.encode(52.232855, 20.9211132)
s2 = pgh.encode(54.5038046, 18.39344)
s3 = pgh.encode(53.013372, 18.5315095)

v = [s1, s2, s3]
# for s in v:
#     print(s)
#
# print(pgh.geohash_haversine_distance(s2, s3)/1000)




# print(pgh.geohash_approximate_distance(s1, s2))
# print(pgh.geohash_haversine_distance(s1, s2)/1000)