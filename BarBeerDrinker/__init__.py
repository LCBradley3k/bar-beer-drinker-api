from flask import Flask
from flask import jsonify
from flask import make_response
from flask import request
import json

from BarBeerDrinker import database

app = Flask(__name__)

#specifies API request for our URL, the HTTP method (what type of request)
@app.route('/api/bar', methods=["GET"])
def get_bars():
    #executes python object, converts to json, and return it
    return jsonify(database.get_bars())

@app.route('/api/drinker', methods=["GET"])
def get_drinkers():
    return jsonify(database.get_drinkers())

@app.route('/api/item', methods=["GET"])
def get_items():
    return jsonify(database.get_items())

#<name> holds name of a given bar
@app.route("/api/bar/<name>", methods=["GET"])
#name needs to match above <name>
def find_bar(name):
    try:
        if name is None:
            raise ValueError("Bar is not specified.")
        bar = database.find_bar(name)
        if bar is None:
            #status codes just indicate some kind of error
            return make_response("No bar found with the given name.", 404)
        return jsonify(bar)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)

@app.route("/api/drinker/<name>", methods=["GET"])
def find_drinker(name):
    try:
        if name is None:
            raise ValueError("Drinker is not specified.")
        drinker = database.find_drinker(name)
        if drinker is None:
            return make_response("No drinker found with the given name.", 404)
        return jsonify(drinker)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)

@app.route("/api/item/<name>", methods=["GET"])
def find_item(name):
    try:
        if name is None:
            raise ValueError("Item is not specified.")
        item = database.find_item(name)
        if item is None:
            return make_response("No item found with the given name.", 404)
        return jsonify(item)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)

@app.route("/api/top-spenders-by-bar", methods=["GET"])
def find_top_spenders(name):
    try:
        if name is None:
            raise ValueError("Bar is not specified.")
        top_spenders = database.find_top_spenders(name)
        if top_spenders is None:
            return make_response("No bar found with the given name.", 404)
        return jsonify(top_spenders)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)

@app.route("/api/transactions", methods=["GET"])
def find_transaction(name):
    try:
        if name is None:
            raise ValueError("Drinker is not specified.")
        transactions = database.find_transaction(name)
        if transactions is None:
            return make_response("No transactions found with the given name.", 404)
        return jsonify(transactions)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)

@app.route("/api/top-items-by-bar", methods=["GET"])
def find_top_items(name):
    try:
        if name is None:
            raise ValueError("Item is not specified.")
        top_items = database.find_top_items(name)
        if top_items is None:
            return make_response("No item found with the given name.", 404)
        return jsonify(top_items)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)

@app.route("/api/top-items-by-drinker", methods=["GET"])
def find_top_bought(name):
    try:
        if name is None:
            raise ValueError("Item is not specified.")
        top_items = database.find_top_bought(name)
        if top_items is None:
            return make_response("No item found with the given name.", 404)
        return jsonify(top_items)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)

@app.route("/api/top-manufacturers-by-bar", methods=["GET"])
def find_top_manf(name):
    try:
        if name is None:
            raise ValueError("Manufacturer is not specified.")
        top_items = database.find_top_manf(name)
        if top_items is None:
            return make_response("No manufacturer found with the given name.", 404)
        return jsonify(top_items)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)

@app.route("/api/top-bars-by-beer", methods=["GET"])
def find_top_bars(name):
    try:
        if name is None:
            raise ValueError("Bar is not specified.")
        top_items = database.find_top_bars(name)
        if top_items is None:
            return make_response("No bar found with the given name.", 404)
        return jsonify(top_items)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)

@app.route("/api/top-drinkers-by-beer", methods=["GET"])
def find_top_drinkers(name):
    try:
        if name is None:
            raise ValueError("Drinker is not specified.")
        top_items = database.find_top_drinkers(name)
        if top_items is None:
            return make_response("No drinker found with the given name.", 404)
        return jsonify(top_items)
    except ValueError as e:
        return make_response(str(e), 400)
    except Exception as e:
        return make_response(str(e), 500)


#find beers cheaper than given number
#used when getting input from user
@app.route("/api/beers_cheaper_than", methods=["POST"])
def find_beers_cheaper_than():
    #takes in string and converts to python object
    body = json.loads(request.data)
    max_price = body['maxPrice']
    return jsonify(database.filter_beers(max_price))