from flask import Flask
from flask import jsonify
from flask import request
from datetime import timedelta
from couchbase.exceptions import DocumentNotFoundException
from couchbase.cluster import Cluster, ClusterOptions, ClusterTimeoutOptions
from couchbase_core.cluster import PasswordAuthenticator
import requests
import sys

timeout_options = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=5), query_timeout=timedelta(seconds=10))

cluster = Cluster('couchbase://10.101.3.79:8091', ClusterOptions(
    PasswordAuthenticator('buckets', 'buckets'), timeout_options=timeout_options))

cb = cluster.bucket('session')
app = Flask(__name__)

offset = int(sys.argv[1]) * 1000
maximum = (int(sys.argv[1]) + 1522) * 1000
while offset <= maximum:
    print("current offset: " + str(offset))
    for row in cb.query("SELECT META().id FROM `session` OFFSET " + str(offset) + " limit 1000;"):
        key = row['id']
        try:
            document = cb.get(key).value[0]
            cb.upsert(key, document)
        except KeyError:
            print(row['id'])
    offset = offset + 1000