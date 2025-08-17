import psycopg2
import time
import csv
from track_resource import Tracker  # Make sure this is your resource tracker

# List of TPC-H queries as strings
tpch_queries = [
    "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND ps_supplycost = ( SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;",  # Query 2
]

def execute_and_measure(query, user, password, dbname):
    conn = psycopg2.connect(host='localhost', user=user, password=password, database=dbname)
    cur = conn.cursor()
    tracker = Tracker()
    tracker.start()
    cur.execute(query)
    conn.commit()
    tracker.stop()
    cpu = tracker.cpu_consumption()
    ram = tracker.ram_consumption()
    disk = tracker.disk_consumption() if hasattr(tracker, 'disk_consumption') else None
    duration = tracker.duration
    cur.close()
    conn.close()
    return cpu, ram, disk, duration

user = 'postgres'
password = 'Ayoub.2002'
dbname = 'Benchmarking'

result = []
for i, query in enumerate(tpch_queries):
    cpu, ram, disk, duration = execute_and_measure(query, user, password, dbname)
    print(f" || Query {i}: CPU={cpu:.3f}, RAM={ram:.3f}, Disk={disk:.3f}, Time={duration:.3f}")

