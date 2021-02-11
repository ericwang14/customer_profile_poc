from flask import Flask
from flask import jsonify
from flask import request
from couchbase.exceptions import DocumentNotFoundException
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator

cluster = Cluster('couchbase://10.101.3.79:8091', ClusterOptions(
    PasswordAuthenticator('buckets', 'buckets')))

cb = cluster.bucket('session')
app = Flask(__name__)


@app.route('/customer_profile/<pc_id>', methods=['GET'])
def hello_world(pc_id):
    try:
        document = cb.get('customer::profile::' + pc_id).value[0]
        return jsonify(document)
    except DocumentNotFoundException as e:
        return ''


@app.route('/customer_profile/<pc_id>', methods=['POST'])
def update(pc_id):
    data = request.get_json()
    key = 'customer::profile::' + pc_id
    print(key)
    try:
        cb.upsert(key, [data])
        return jsonify(data)
    except DocumentNotFoundException as e:
        print(e)
        return ''
