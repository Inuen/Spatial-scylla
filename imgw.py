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
    'create table if not exists imgw2(id int, station int, city text, parameter text, date text, value float, hash text, wkt text, primary key(id))')

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    EPSG_2180 = Proj(init='EPSG:2180')
    EPSG_4326 = Proj(init='EPSG:4326')


# pl 48-55 14-24


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
    INSERT INTO imgw2(id, station, city, parameter, date, value, hash, wkt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""")

station_codes, station_coord, station_hash = read_full_stations()
path = 'D:\\Studia\\inz\\imgw\\'
paths = []
with os.scandir('D:\\Studia\\inz\\imgw') as entries:
    for entry in entries:
        if 'B00608S' in entry.name:  # suma opadu 10 min
            paths.append(path + entry.name)

paths = paths[::-1]
data = []
for path in paths:
    i = 0
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        for row in csv_reader:
            if row[0] in station_codes:
                coord = pgh.decode(station_hash[row[0]])
                table = [row[0], station_codes[row[0]], row[1], row[2], row[3], station_hash[row[0]], f"POINT({coord[0]} {coord[1]})", ]
                data.append(table)
                i += 1
        print(i)

#####????? tab czy nie tab
start = time.time()
cnt = 0
for row in data:
    query = """
        INSERT INTO imgw2(id, station, city, parameter, date, value, hash, wkt)
        VALUES (%i, %i, '%s', '%s', '%s', %f, '%s', '%s');""" % (int(cnt), int(row[0]), str(row[1]), str(row[2]), str(row[3]), float(row[4].replace(',', '.')), str(row[5]), str(row[6]))
    tst = [int(cnt), int(row[0]), str(row[1]), str(row[2]), str(row[3]), float(row[4].replace(',', '.')), str(row[5]), str(row[6])]
    cnt += 1
    # b = session.execute(query)
    session.execute_async(new_rain_insert, tst)
print(cnt)
now = time.time()
print(now - start)
print()
