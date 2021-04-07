import pygeohash as pgh
from pyproj import Proj, transform
import warnings
from cassandra.cluster import Cluster
import csv
import os
from geopy.geocoders import Nominatim
import sys
import time

# from cassandra.policies import DCAwareRoundRobinPolicy
# from cassandra.auth import PlainTextAuthProvider


# cluster = Cluster(['192.168.35.237'])
cluster = Cluster(['localhost'], port=9082)
session = cluster.connect('scylla')

table = session.execute(
    'create table if not exists imgw_rain(id int, station int, city text, parameter text, date text, rain float, hash text, primary key(id))')

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    EPSG_2180 = Proj(init='EPSG:2180')
    EPSG_4326 = Proj(init='EPSG:4326')


# pl 48-55 14-24


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


def fill_stations():
    station_codes = {}
    with open("D:\\Studia\\inz\\imgw\\kody_stacji.csv") as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            station_codes[row[1]] = row[2]

    station_coord = {}
    station_hash = {}
    locator = Nominatim(user_agent="michalnguyen99@gmail.com")
    fails = []
    for station in station_codes:
        try:
            location = locator.geocode(station_codes[station])
            coord = (location.latitude, location.longitude)
            geohash = pgh.encode(location.latitude, location.longitude)
            station_coord[station] = coord
            station_hash[station] = geohash
        except AttributeError:
            fails.append(station)
            tb = sys.exc_info()
            print(tb)
            print("FAIL\t", station_codes[station])

    for station in fails:
        del station_codes[station]

    with open('D:\\Studia\\inz\\imgw\\kody_stacji_full.csv', 'w') as file:
        for station in station_codes:
            line = f'{station};{station_codes[station]};{station_coord[station]};{station_hash[station]}\n'
            file.write(line)

    return station_codes, station_coord, station_hash


def read_full_stations():
    station_codes = {}
    station_coord = {}
    station_hash = {}
    with open('D:\\Studia\\inz\\imgw\\kody_stacji_full.csv') as file:
        csv_reader = csv.reader(file, delimiter=';')
        for row in csv_reader:
            station_codes[row[0]] = row[1]
            station_coord[row[0]] = row[2]
            station_hash[row[0]] = row[3]

    return station_codes, station_coord, station_hash


new_rain_insert = session.prepare("""
    INSERT INTO imgw_rain(id, station, city, parameter, date, rain, hash)
    VALUES (?, ?, ?, ?, ?, ?, ?)""")

station_codes, station_coord, station_hash = read_full_stations()
path = 'D:\\Studia\\inz\\imgw\\'
paths = []
with os.scandir('D:\\Studia\\inz\\imgw') as entries:
    for entry in entries:
        if 'B00608S' in entry.name:  # suma opadu 10 min
            paths.append(path + entry.name)

path = paths[0]
data = []
with open(path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    for row in csv_reader:
        if row[0] in station_codes:
            table = [row[0], station_codes[row[0]], row[1], row[2], row[3], station_hash[row[0]], ]
            data.append(table)

start = time.time()
cnt = 0
for row in data:
    tst = [int(cnt), int(row[0]), str(row[1]), str(row[2]), str(row[3]), float(row[4].replace(',', '.')), str(row[5])]
    cnt += 1
    # session.execute(new_rain_insert, tst)  # a lot of seconds
    session.execute_async(new_rain_insert, tst) # 203 s

now = time.time()
print(now - start)
# h = hashing_array(v)
# print(hash_prefix(h))
