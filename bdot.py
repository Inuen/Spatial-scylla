import warnings
from pyproj import Proj, transform
from cassandra.cluster import Cluster
import xml.etree.ElementTree as ET
import pygeohash as pgh
import time

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    EPSG_2180 = Proj(init='EPSG:2180')
    EPSG_4326 = Proj(init='EPSG:4326')

cluster = Cluster(['localhost'], port=9082)
session = cluster.connect('scylla')


bdot_path = r"D:\Studia\inz\bdot\BDOT10k\PL.PZGiK.336.2262__OT_OIPR_P.xml"


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
                        child_to_attribute(fc3, attributes, attributes_d, row)

    split_coord = row['pos'].split(' ')
    x, y = split_coord[0], split_coord[1]
    lon, lat = transform(EPSG_2180, EPSG_4326, x, y)
    geohash = pgh.encode(lat, lon)
    row['hash'] = geohash
    attributes.append('hash')
    attributes_d['hash'] = 'text'
    data.append(row)



# print(data)
# print(attributes_d)
# print(attributes, len(attributes))
# id int, station int, city text, parameter text, date text, rain float, hash text, primary key(id))

table_string = 'create table if not exists test_oipr('
for attr in attributes_d:
    table_string += attr + f' {attributes_d[attr]}, '

table_string = table_string + 'primary key(lokalnyId));'
print(table_string)
table = session.execute(table_string)
prefix_str = 'INSERT INTO test_oipr('
start = time.time()
check = open('log.txt', 'w')
for row in data:
    values_str = ''
    for feature in row:
        if feature == 'hash':
            prefix_str += feature + ')'
        else:
            prefix_str += feature + ', '
        if attributes_d[feature] == 'text':
            values_str += f"'{row[feature]}'" + ', '
        else:
            values_str += row[feature] + ', '
    prefix_str += ' VALUES ('
    values_str = values_str[:-2] + ');'

    # print(prefix_str + values_str)
    new_rain_insert = session.execute(f"""{prefix_str + values_str}""")
    check.write(new_rain_insert)

now = time.time()
print(now-start)
