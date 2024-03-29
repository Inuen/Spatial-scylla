import warnings
from pyproj import Proj, transform
from cassandra.cluster import Cluster
import xml.etree.ElementTree as ET
import pygeohash as pgh
import time
from geohash_utils import *

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    EPSG_2180 = Proj(init='EPSG:2180')
    EPSG_4326 = Proj(init='EPSG:4326')

cluster = Cluster(['localhost'], port=9082)
session = cluster.connect('scylla2')


bdot_path = r"D:\Studia\inz\bdot\BDOT10k\PL.PZGiK.336.2262__OT_KUHO_A.xml"


tree = ET.parse(bdot_path)
root = tree.getroot()


def child_to_attribute(feature, all_attributes, types, record):
    prefix_len = feature.tag.find('}') + 1
    tag = feature.tag[prefix_len:]
    record[tag] = feature.text
    if tag not in all_attributes:
        all_attributes.append(tag)
        if feature.text == 'true' or feature.text == 'false':
            types[tag] = 'boolean'
        elif '.' in feature.text and feature.text.replace('.', '').isnumeric():
            types[tag] = 'float'
        elif feature.text.isnumeric():
            types[tag] = 'int'
        else:
            types[tag] = 'text'


attributes = []
attributes_d = {}
data = []
for child in root:
    row = {}
    for fc in child[0]:
        if fc.text is not None:
            if fc.text.strip() != '':
                child_to_attribute(fc, attributes, attributes_d, row)

        if fc.attrib is not None:
            for fc2 in fc:
                if fc2.text.strip() != '':
                    child_to_attribute(fc2, attributes, attributes_d, row)
                else:
                    for fc3 in fc2:
                        if fc3.attrib is not None:
                            if fc3.text.strip() != '':
                                child_to_attribute(fc3, attributes, attributes_d, row)
                            else:
                                for fc4 in fc3:
                                    if fc4.attrib is not None:
                                        for fc5 in fc4:
                                            if fc5.text.strip() != '':
                                                child_to_attribute(fc5, attributes, attributes_d, row)
                                    if fc4.text.strip() != '':
                                        child_to_attribute(fc4, attributes, attributes_d, row)
                        else:
                            child_to_attribute(fc3, attributes, attributes_d, row)
    if 'pos' in row:
        split_coord = row['pos'].split(' ')
        x, y = split_coord[0], split_coord[1]
        lon, lat = transform(EPSG_2180, EPSG_4326, x, y)
        geohash = pgh.encode(lat, lon)
        row['wkt'] = f'POINT( {lon} {lat} )'
        row['hash'] = geohash
        attributes.append('hash')
        attributes_d['hash'] = 'text'
        attributes.append('wkt')
        attributes_d['wkt'] = 'text'
        data.append(row)
    elif 'posList' in row:
        split_coord = row['posList'].split(' ')
        coordinates = []
        centroid = [0, 0]
        for coord in range(0, len(split_coord), 2):
            x = split_coord[coord]
            y = split_coord[coord + 1]
            lon, lat = transform(EPSG_2180, EPSG_4326, x, y)
            coordinates.append([lon, lat])
            centroid[0] += lon
            centroid[1] += lat
        centroid[0] /= len(split_coord)
        centroid[1] /= len(split_coord)
        centroid[0] *= 2
        centroid[1] *= 2
        wkt = coord_to_wkt_polygon(coordinates)
        row['wkt'] = wkt
        attributes.append('wkt')
        attributes_d['wkt'] = 'text'
        encoded_coord = pgh.encode(centroid[1], centroid[0], precision=12)
        for i in range(1, 4):
            name = 'hash' + str(i)
            row[name] = encoded_coord[(i - 1) * 4 : i * 4]  # centroid
            attributes.append(name)
            attributes_d[name] = 'text'
        data.append(row)


table_string = 'create table if not exists kuho('
for attr in attributes_d:
    table_string += attr + f' {attributes_d[attr]}, '

table_string = table_string + 'primary key(lokalnyId));'
print(table_string)
table = session.execute(table_string)
time.sleep(2)
prefix_origin = 'INSERT INTO kuho('
start = time.time()
check = open('log.txt', 'w')
for row in data:
    prefix_str = prefix_origin
    values_str = ''
    for feature in row:
        if feature == 'hash3':
            prefix_str += feature + ')'
        else:
            prefix_str += feature + ', '
        if attributes_d[feature] == 'text':
            values_str += f"'{row[feature]}'" + ', '
        else:
            values_str += row[feature] + ', '
    prefix_str += ' VALUES ('
    values_str = values_str[:-2] + ');'

    print(prefix_str + values_str)
    new_rain_insert = session.execute(f"""{prefix_str + values_str}""")

now = time.time()
print(now-start)
check.close()
