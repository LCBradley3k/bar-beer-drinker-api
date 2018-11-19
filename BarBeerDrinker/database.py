from sqlalchemy import create_engine
from sqlalchemy import sql

from BarBeerDrinker import config

#can be used to run all queries
engine = create_engine(config.database_uri)

#grabs all bars
def get_bars():
    with engine.connect() as con:
        rs = con.execute("SELECT name, license, address, city, state, phone, opening_hour, closing_hour FROM bars;")
        return [dict(row) for row in rs]

#grabs all drinkers
def get_drinkers():
    with engine.connect() as con:
        rs = con.execute("SELECT name, phone, address, state FROM drinkers;")
        return [dict(row) for row in rs]

#grabs all items
def get_items():
    with engine.connect() as con:
        rs = con.execute("SELECT item, category, manf FROM items;")
        return [dict(row) for row in rs]

#find transaction from a drinker
def find_transaction(name):
    with engine.connect() as con:
        query = sql.text(
            "SELECT orders.transaction_id, orders.name, orders.bar_name, transactions.date, transactions.time FROM orders INNER JOIN transactions ON orders.transaction_id = transactions.transaction_id WHERE orders.name = :name GROUP BY transactions.transaction_id ORDER BY transactions.date, transactions.time, orders.bar_name;"
        )
        rs = con.execute(query, name=name)
        result = [dict(row) for row in rs]
        if result is None:
                return None
        return result

#takes in name, find all information for a bar
def find_bar(name):
    with engine.connect() as con:
        query = sql.text(
            "SELECT name, license, address, city, state, phone, opening_hour, closing_hour FROM bars WHERE name = :name;"
        )
        #specify the value, name
        rs = con.execute(query, name=name)
        #just get the first bar
        result = rs.first()
        #need to check for none
        if result is None:
                return None
        return dict(result)

#takes in item, find all information for an item
def find_item(name):
    with engine.connect() as con:
        query = sql.text(
            "SELECT item, category, manf FROM items WHERE item = :name;"
        )
        #specify the value, name
        rs = con.execute(query, name=name)
        #just get the first bar
        result = rs.first()
        #need to check for none
        if result is None:
                return None
        return dict(result)

#takes in name, find all information for a drinker
def find_drinker(name):
    with engine.connect() as con:
        query = sql.text(
            "SELECT name, phone, address, state FROM drinkers WHERE name = :name;"
        )
        rs = con.execute(query, name=name)
        result = rs.first()
        if result is None:
                return None
        return dict(result)

#finds top 10 spenders for given bar
def find_top_spenders(name):
    with engine.connect() as con:
        query = sql.text(
                "SELECT orders.name, orders.bar_name, SUM(transactions.total) as total_spent FROM orders INNER JOIN transactions ON orders.transaction_id = transactions.transaction_id WHERE orders.bar_name = :name GROUP BY orders.name ORDER BY total_spent DESC LIMIT 10;"
        )
        rs = con.execute(query, name=name)
        result = [dict(row) for row in rs]
        if result is None:
                return None
        return result

#finds top 10 items by drinker
def find_top_bought(name):
    with engine.connect() as con:
        query = sql.text(
                "SELECT name, beer, Count(beer) as total_bought FROM orders WHERE name = :name GROUP BY beer ORDER BY total_bought DESC LIMIT 10;"
        )
        rs = con.execute(query, name=name)
        result = [dict(row) for row in rs]
        if result is None:
                return None
        return result

#finds top 10 items by bar
def find_top_items(name):
    with engine.connect() as con:
        query = sql.text(
                "SELECT bar_name, beer, Count(beer) as total_bought FROM orders WHERE bar_name = :name GROUP BY beer ORDER BY total_bought DESC LIMIT 10;"
        )
        rs = con.execute(query, name=name)
        result = [dict(row) for row in rs]
        if result is None:
                return None
        return result

#finds top 10 manufacturers by bar
def find_top_manf(name):
    with engine.connect() as con:
        query = sql.text(
                "SELECT bar_name, manf, COUNT(manf) as total_manf FROM orders INNER JOIN items ON orders.beer = items.item WHERE bar_name = :name GROUP BY manf ORDER BY total_manf DESC LIMIT 10;"
        )
        rs = con.execute(query, name=name)
        result = [dict(row) for row in rs]
        if result is None:
                return None
        return result

#finds top bars by beer
def find_top_bars(name):
    with engine.connect() as con:
        query = sql.text(
                "SELECT bar_name, beer, COUNT(beer) as beers_bought FROM orders WHERE beer = :name GROUP BY bar_name ORDER BY beers_bought DESC LIMIT 10;"
        )
        rs = con.execute(query, name=name)
        result = [dict(row) for row in rs]
        if result is None:
                return None
        return result

#find top drinkers by beer
def find_top_drinkers(name):
    with engine.connect() as con:
        query = sql.text(
                "SELECT name, beer, COUNT(name) as drinker_beers_bought FROM orders WHERE beer = :name GROUP BY name ORDER BY drinker_beers_bought DESC LIMIT 10"
        )
        rs = con.execute(query, name=name)
        result = [dict(row) for row in rs]
        if result is None:
                return None
        return result

# distribution of drinkers at bar in May
def find_drinker_bars_per_hour(name):
        with engine.connect() as con:
                query = sql.text(
                        "SELECT EXTRACT(MONTH FROM date) as month, orders.transaction_id, orders.name, orders.bar_name, SUM(price) as total_per_month FROM orders INNER JOIN transactions ON orders.transaction_id = transactions.transaction_id WHERE orders.name = :name AND EXTRACT(MONTH FROM date) = '5' GROUP BY month, orders.bar_name ORDER BY month, transactions.date, transactions.time;"
                )
                rs = con.execute(query, name=name)
                results = [dict(row) for row in rs]
                for r in results:
                        r['total_per_month'] = float(r['total_per_month'])
                return results


# distribution of beer sales per month
def find_beer_sales_per_month(name):
        with engine.connect() as con:
                query = sql.text("SELECT beer, EXTRACT(MONTH FROM A.date) as month, COUNT(*) as beers_sold FROM ( SELECT beer, total, date, orders.transaction_id as orders_id, transactions.transaction_id as transactions_id FROM orders INNER JOIN transactions ON orders.transaction_id = transactions.transaction_id WHERE orders.beer = :name) A GROUP BY month;")
                rs = con.execute(query, name=name)
                results = [dict(row) for row in rs]
                return results

# distribution of bar sales per hour
def find_bar_sales_per_hour(name):
        with engine.connect() as con:
                query = sql.text(
                        "SELECT bar_name, EXTRACT(HOUR FROM A.time) as hour, SUM(total) as total_per_hour FROM ( SELECT bar_name, total, time, orders.transaction_id as orders_id, transactions.transaction_id as transactions_id FROM orders INNER JOIN transactions ON orders.transaction_id = transactions.transaction_id WHERE orders.bar_name = :name) A GROUP BY hour;"
                )
                rs = con.execute(query, name=name)
                results = [dict(row) for row in rs]
                #for r in results:
                 #       r['total'] = float(r['total'])
                return results

# verify that there is no transactions outside of opening and closing times
def verify_times():
        with engine.connect() as con:
                query = sql.text("SELECT NOT EXISTS (SELECT * FROM (SELECT DISTINCT(transactions.Transaction_ID), CAST(opening_hour AS TIME) as opening_hour, CAST(transactions.time as TIME) as transaction_time, CAST(closing_hour AS TIME) as closing_hour FROM transactions RIGHT JOIN orders ON transactions.Transaction_ID = orders.transaction_ID JOIN bars ON orders.Bar_name = bars.Name LIMIT 1000) A WHERE A.transaction_time BETWEEN opening_hour AND closing_hour) AS CheckTimes;")
                rs = con.execute(query)
                results = [dict(row) for row in rs]
                return results

def verify_states():
        with engine.connect() as con:
                query = sql.text("SELECT NOT EXISTS(SELECT frequents.name, frequents.bar, bars.state, drinkers.state FROM frequents Inner Join drinkers on frequents.name = drinkers.name Inner Join bars on frequents.bar = bars.name WHERE drinkers.state <> bars.state) AS CheckStates;")
                rs = con.execute(query)
                results = [dict(row) for row in rs]
                return results

def verify_prices():
        with engine.connect() as con:
                query = sql.text("SELECT NOT EXISTS(SELECT A.valid_price FROM(SELECT CASE WHEN(s1.price-s2.price = 0 OR s1.price-s2.price > 0 OR s1.price-s2.price < 0) THEN 'TRUE' else 'FALSE' END AS valid_price FROM sells_beer s1 JOIN sells_beer s2 WHERE (s1.beer != s2.beer) AND (s1.bar != s2.bar) ORDER BY s1.beer, s1.bar, s2.beer) AS A WHERE A.valid_price = 'False') AS check_prices;")
                rs = con.execute(query)
                results = [dict(row) for row in rs]
                return results

#just name example, have to edit to fit our needs
def filter_beers(max_price):
        with engine.connect() as con:
                query = sql.text(
                        "SELECT * FROM sells WHERE price < :max_price;"
                )

                rs = con.execute(query, max_price=max_price)
                results = [dict(row) for row in rs]
                #we need to make sure it is store in python correctly, so it shows up correctly in json
                for r in results:
                        r['price'] = float(r['price'])
                return results