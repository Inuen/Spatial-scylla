from cassandra.cluster import Cluster
import time

cluster = Cluster(['localhost'], port=9080)
# cluster = Cluster(['192.168.35.237'])
session = cluster.connect('scylla')
start = time.time()
query = session.execute("select * from new_rain2 where hash like 'u2%' allow filtering;")
print(time.time()-start)
for row in query:
    print(f'{row[0]}    {row[1]}   {row[2]}     {row[3]}    {row[4]}    {row[5]}    {row[6]}')
    break