from flask import Flask
from flask import jsonify
from flask import request
from couchbase.exceptions import DocumentNotFoundException
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
import requests

cluster = Cluster('couchbase://10.101.3.79:8091', ClusterOptions(
    PasswordAuthenticator('buckets', 'buckets')))

cb = cluster.bucket('session')
app = Flask(__name__)


@app.route('/customer_profile/<pc_id>', methods=['GET'])
def get(pc_id):
    try:
        document = cb.get('customer::profile::' + pc_id).value
        filter_param = request.args.get('filter')
        if filter_param == 'FavoriteProducts' and 'FavoriteProducts' in document:
            prod_id = document['FavoriteProducts']
            r = requests.get('https://api2.shop.com/product-service/v1/Product/ProdId/' +
                             prod_id + '?fl=product')
            document['products'] = r.text
        return jsonify(document)
    except DocumentNotFoundException as e:
        return ''


@app.route('/customer_profile/<pc_id>', methods=['POST'])
def update(pc_id):
    data = request.get_json()
    key = 'customer::profile::' + pc_id
    print(key)
    try:
        cb.upsert(key, data)
        return jsonify(data)
    except DocumentNotFoundException as e:
        print(e)
        return ''
