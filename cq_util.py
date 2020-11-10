import requests
import os
from http import HTTPStatus
import json
BASE_URL = 'https://dev.cleanqueen.cl/'
global AUTH_TOKEN

def configure_auth_token():
    global AUTH_TOKEN
    AUTH_TOKEN = "Y2tfMmUyNTEzNjExZmY4NjE2MDY0ZjA4NDE3MWE5MTQxZjg2MTg0NWMzZjpjc19lYTNjYjQ2MWU2YjVhY2NkY2YwYjZmMGYxYjcwMGQ0ZDczZWZjYWRj"
class OrderStatus:
    AWAITING_SHIPMENT = 'awaiting-shipment'
    SHIPPED = 'shipped'
    COMPLETED = 'completed'
    
def update_order(order_id, new_status, eta):
    url = "https://dev.cleanqueen.cl/wp-json/wc/v3/orders/{}".format(order_id);
    headers = {'content-type': 'application/json',
        'Authorization': 'Basic {}'.format(AUTH_TOKEN)}
    payload = {
        "status": new_status,
        'meta_data': [
            {"key": "_delivery_date", "value": eta}
        ]}
    r = requests.post(url, data = json.dumps(payload), headers=headers)
    if r.status_code == HTTPStatus.OK:
        return True
    else:
        return False

def check_if_order_exists_remote(order_id):
    url = "https://dev.cleanqueen.cl/wp-json/wc/v3/orders/{}".format(order_id);
    headers = {'content-type': 'application/json',
        'Authorization': 'Basic {}'.format(AUTH_TOKEN)}
    try:
        r = requests.get(url, headers=headers)
        return r.status_code == HTTPStatus.OK
    except:
        pass

def get_orders(ids_arr):
    url = "https://dev.cleanqueen.cl/wp-json/wc/v3/orders/";
    params = {'include': ids_arr}
    headers = {'content-type': 'application/json',
        'Authorization': 'Basic {}'.format(AUTH_TOKEN)}
    try:
        r = requests.get(url, headers=headers, params= params)
        return r.json()
    except:
        pass


configure_auth_token()