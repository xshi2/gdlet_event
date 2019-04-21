from flask import Flask, flash, redirect, render_template, request, session, abort, send_from_directory
from random import randint
from configs import db_config, google_map_config
from country_location import country_locations
import psycopg2
import os
import gmplot
import googlemaps
import requests 
import random
from bs4 import BeautifulSoup 
from datetime import datetime, timedelta
from pytz import timezone


def get_connection():
    connection = None
    try:
        connection = psycopg2.connect(host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password'])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return connection

def get_data(category, event_date):
    connection = get_connection()

    results = []

    if connection:
        cursor = connection.cursor()
        query = (
                """
                SELECT cyear, country, 1, sum(nummentions) as nummentions, avg(gs) as gs,avg(tone) as tone FROM category_events e inner join category_name n on e.actor_type = n.category_code WHERE n.category_name ='{}' and cyear = '{}' group by cyear, country order by country desc
                """.format(category,event_date.strftime('%Y%m%d'))
        )

        cursor.execute(query)
        results = cursor.fetchall()

        cursor.close()
        connection.close()

    return results

def get_top_ten():
    connection = get_connection()
    pages = []

    if connection:
        now = datetime.utcnow().date()
        cursor = connection.cursor()

        query = (
                """
                SELECT page_title,max(page_description) as page_description,max(page_image_url) as page_image_url,sum(nummentions) as nummentions, mention_source FROM category_events where page_title <> 'None' and cyear = '{}' group by mention_source,page_title order by sum(nummentions) DESC LIMIT 10
                """.format(now.strftime('%Y%m%d'))
        )

        cursor.execute(query)
        pages = cursor.fetchall()

        cursor.close()
        connection.close()
    return pages


def get_categories():
    connection = get_connection()
    categories = []

    if connection:
        cursor = connection.cursor()
        query = (
                """
                SELECT distinct(category_name) FROM category_name order by 1
                """
        )

        cursor.execute(query)
        result = cursor.fetchall()
        
        for item in result:
            categories.append(item[0])

        cursor.close()
        connection.close()

    return categories

app = Flask(__name__)

@app.route("/")
def index():
    news = get_top_ten()
    categories = get_categories()
    today = datetime.utcnow().date().strftime('%m/%d/%Y')
    # news = []
    # categories = []
    response = render_template('home.html',**locals())
    return response

@app.route("/rendermap")
def render_map():
    fname = request.args.get('file')
    response = render_template(fname)
    return response
    

@app.route("/map")
def map():
    category = request.args.get('category')

    try:
        event_date = datetime.strptime(request.args.get('event_date'), '%m/%d/%Y')
    except:
        event_date = datetime.utcnow().date()
    
    data = get_data(category, event_date)

    lats = []
    lons = []
    weights = []

    gmap = gmplot.GoogleMapPlotter(32.7884336, -30.438481, 3)
    gmap.apikey = google_map_config['apikey']

    for item in data:
        if item[1] in country_locations:
            location = country_locations[item[1]]
    
            tone = item[5]
            if tone < -2:
                start_color = (250, 0, 0, 0)
                end_color = (250, 0, 0, 1)
            elif tone > 2:
                start_color = (0, 250, 0, 0)
                end_color = (0, 250, 0, 1)
            else:
                start_color = (255, 255, 0, 0)
                end_color = (255, 255, 0, 1)

            rad = item[3]
            if rad > 10:
                rad = 10

            gmap.heatmap([location[0]], [location[1]], threshold=4, radius=rad, dissipating=False, gradient=[start_color, end_color, end_color]) 
    n = random.randint(1, 999999999)
    map_file_name = "maps/map_{}.html".format(n)
    gmap.draw(map_file_name)

    response = render_template('map.html',**locals())
    return response

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)

