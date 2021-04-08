import warnings
from pyproj import Proj, transform
from cassandra.cluster import Cluster
import xml.etree.ElementTree as ET
import pygeohash as pgh

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    EPSG_2180 = Proj(init='EPSG:2180')
    EPSG_4326 = Proj(init='EPSG:4326')

cluster = Cluster(['localhost'], port=9082)
session = cluster.connect('scylla')


bdot_path = r"D:\Studia\inz\bdot\BDOT10k\PL.PZGiK.336.2262__OT_OIPR_P.xml"


tree = ET.parse(bdot_path)
root = tree.getroot()

attributes = []
attributes_d = {}
data = []
for child in root:
    row = {}
    for fc in child[0]:
        if fc.text is not None:
            if fc.text.strip() != '':
                prefix_len = fc.tag.find('}') + 1
                tag = fc.tag[prefix_len:]
                # print(tag, fc.text)
                row[tag] = fc.text
                if tag not in attributes:
                    attributes.append(tag)
                    if fc.text == 'true' or fc.text == 'false':
                        attributes_d[tag] = 'boolean'
                    elif '.' in fc.text and fc.text.replace('.', '').isnumeric():
                        attributes_d[tag] = 'float'
                    elif fc.text.isnumeric():
                        attributes_d[tag] = 'int'
                    else:
                        attributes_d[tag] = 'text'

        if fc.attrib is not None:
            for fc2 in fc:
                if fc2.text.strip() != '':
                    prefix_len = fc2.tag.find('}') + 1
                    tag = fc2.tag[prefix_len:]
                    # print(tag, fc2.text)
                    row[tag] = fc2.text
                    if tag not in attributes:
                        attributes.append(tag)
                        if fc.text == 'true' or fc.text == 'false':
                            attributes_d[tag] = 'boolean'
                        elif '.' in fc.text and fc.text.replace('.', '').isnumeric():
                            attributes_d[tag] = 'float'
                        elif fc.text.isnumeric():
                            attributes_d[tag] = 'int'
                        else:
                            attributes_d[tag] = 'text'
                else:
                    for fc3 in fc2:
                        prefix_len = fc3.tag.find('}') + 1
                        tag = fc3.tag[prefix_len:]
                        # print(tag, fc3.text)
                        row[tag] = fc3.text
                        if tag not in attributes:
                            attributes.append(tag)
                            if fc.text == 'true' or fc.text == 'false':
                                attributes_d[tag] = 'boolean'
                            elif '.' in fc.text and fc.text.replace('.', '').isnumeric():
                                attributes_d[tag] = 'float'
                            elif fc.text.isnumeric():
                                attributes_d[tag] = 'int'
                            else:
                                attributes_d[tag] = 'text'
    data.append(row)
    break




print(data)
print(attributes_d)
# print(attributes, len(attributes))
# id int, station int, city text, parameter text, date text, rain float, hash text, primary key(id))
table_string = 'create table if not exists test_bdot('
for attr in attributes:
    table_string += attr + ' text, ' ####################### zmienić typowanie atrybutów
# print(table_string + 'primary key(lokalnyId))')
# table = session.execute(table_string)


