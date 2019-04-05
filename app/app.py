from flask import Flask, flash, redirect, render_template, request, session, abort, send_from_directory
from random import randint
from configs import db_config
import psycopg2
import os
import gmplot
import googlemaps


def get_data(category):
    try:
        connection = psycopg2.connect(host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password'])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    cursor = connection.cursor()

    if category == None:
        query = (
                """
                SELECT cyear, country, actor_type, sum(nummentions) as nummentions, sum(gs) as gs,avg(tone) as tone FROM category_events group by cyear, country, actor_type
                """
        )
    else:
        query = (
                """
                SELECT cyear, country, 1, sum(nummentions) as nummentions, sum(gs) as gs,avg(tone) as tone FROM category_events WHERE actor_type='{}' group by cyear, country order by country desc LIMIT 30
                """.format(category)
        )

    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()
    connection.close()

    return results

app = Flask(__name__)

@app.route("/")
def index():
    data = get_data(None)
    response = render_template('home.html',**locals())
    return response

@app.route("/search")
def search():
    try:
        connection = psycopg2.connect(host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password'])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    cursor = connection.cursor()

    query = (
            """
            SELECT distinct(actor_type) FROM category_events
            """
    )

    cursor.execute(query)
    result = cursor.fetchall()
    categories = []
    for item in result:
        categories.append(item[0])

    cursor.close()
    connection.close()

    response = render_template('search.html',**locals())
    return response
    
@app.route("/map")
def map():
    category = request.args.get('category')

    data = get_data(category)

    lats = []
    lons = []
    weights = []

    gmap = gmplot.GoogleMapPlotter(32.7884336, -30.8033618, 3)
    gmap.apikey = ""

    for item in data:
        if item[1] in country_locations:
            location = country_locations[item[1]]
            # print(item[1])
            # print(location)
            # geocode_result = gmaps.geocode(item[1])
            # geom = geocode_result[0]['geometry']
            # loc = geom['location']
    
            tone = item[5]
            if tone < 0:
                start_color = (250, 0, 0, 0)
                end_color = (250, 0, 0, 1)
            else:
                start_color = (0, 250, 0, 0)
                end_color = (0, 250, 0, 1)

            rad = item[3]
            if rad > 10:
                rad = 10

            gmap.heatmap([location[0]], [location[1]], threshold=4, radius=rad, dissipating=False, gradient=[start_color, end_color, end_color])
            # lats.append(location[0])
            # lons.append(location[1])
            # weights.append(item[3])
    
    gmap.draw("map.html")

    response = render_template('map.html')
    return response

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
