import pygeohash as pgh
from pyproj import Proj, transform
import warnings
from cassandra.cluster import Cluster
from xml.dom import minidom

cluster = Cluster(['localhost'], port=9082)
session = cluster.connect('scylla')

bdot_path = r"D:\Studia\inz\bdot\BDOT10k\PL.PZGiK.336.2262__OT_OIPR_P.xml"
# file = minidom.parse(bdot_path)
# models = file.getElementsByTagName('gml:featureMember')
# print(models[1].attributes)
# with open(bdot_path, 'r') as file:
#     data = file.read()

import xml.etree.ElementTree as ET


tree = ET.parse(bdot_path)

# getting the parent tag of
# the xml document
root = tree.getroot()
for child in root:
    for fc in child[0]:
        if fc.text is not None:
            if fc.text.strip() != '':
                prefix_len = fc.tag.find('}') + 1
                print(fc.tag[prefix_len:], fc.text)
        if fc.attrib is not None:
            for fc2 in fc:
                if fc2.text.strip() != '':
                    prefix_len = fc2.tag.find('}') + 1
                    print(fc2.tag[prefix_len:], fc2.text)
                else:
                    for fc3 in fc2:
                        prefix_len = fc3.tag.find('}') + 1
                        print(fc3.tag[prefix_len:], fc3.text)

    break

