from flask import Flask, render_template, request
import pymongo as mongo
import sys
from flask.ext.mysql import MySQL
from sql import *
mongo_client = mongo.MongoClient(
    "mongodb://cis550mongo:cis550project@mongocluster-shard-00-00-5qypc.mongodb.net:27017,"
    "mongocluster-shard-00-01-5qypc.mongodb.net:27017,"
    "mongocluster-shard-00-02-5qypc.mongodb.net:27017/test?ssl=true&replicaSet=mongocluster-shard-0&authSource=admin")
extra = mongo_client.actors
basic = mongo_client.actor_info

app = Flask(__name__)

mysql = MySQL()
# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'feedmenews'
app.config['MYSQL_DATABASE_PASSWORD'] = '12345678'
app.config['MYSQL_DATABASE_DB'] = 'news'
app.config['MYSQL_DATABASE_HOST'] = 'mydbfeedmenews.cffkuryafwgu.us-east-1.rds.amazonaws.com'
mysql.init_app(app)
cursor = mysql.connect().cursor()

male_extra = extra.male
female_extra = extra.female
female_basic = basic.male
male_basic = basic.female


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/result', methods=['POST'])
def search():
    category = request.form.getlist("category")
    Like = request.form.get("Like")
    result, recommend_actor, valid = recommend_query(category, conn, type_name, Like)
    if valid is None:
        return render_template('error.html')
    else:
        return render_template('result.html', results=result, people=recommend_actor)
    return


def movies_acted_in(actor_name):
    sql = "SELECT m.genres, m.primaryTitle, p.primaryName \
            FROM movies m JOIN act_in a JOIN people p \
            ON m.movie_id = a.movie_id AND p.people_id = a.people_id \
            WHERE p.primaryName = '" + actor_name + "';"
    cursor.execute(sql)
    results = cursor.fetchall()
    return render_template('', results=results)


@app.route('/search')
def actor_search():
    return render_template('search.html', message=None)


def hash_name(actor_name):
    if "(" in actor_name:
        actor_name = actor_name[0:actor_name.find("(")]
    return actor_name.replace(" ", "").lower().replace("-", "").replace(".", "").replace("'", "")


@app.route('/search_actors', methods=['POST'])
def actor_results():
    actor_name = request.form['name']
    actor_sex = request.form['sex']
    # print(actor_name , file=sys.stderr)
    # print(actor_sex, file=sys.stderr)
    return find_actor(actor_name, actor_sex)


@app.route('/detail/<name>', methods=['GET', 'POST'])
def detail(name):
    cursor.execute("SELECT gender from people \
                    WHERE edit_primaryName = '"+name+"';")
    result = cursor.fetchone();
    print(result, file=sys.stderr)
    if result:
        return find_actor(name, result[0])
    else:
        return render_template('error.html')


def find_actor(actor_name, actor_sex):
    if actor_name == "undefined" and actor_sex is None:
        return render_template('search.html', message="Did not receive search parameters!")
    if actor_sex == "male":
        actor_obj = male_extra.find_one({"_id": hash_name(actor_name)})
        actor_basic = male_basic.find_one({"_id": hash_name(actor_name)})
    elif actor_sex == "female":
        actor_obj = female_extra.find_one({"_id": hash_name(actor_name)})
        actor_basic = female_basic.find_one({"_id": hash_name(actor_name)})
    else:
        actor_obj = None
        actor_basic = None
    print(hash_name(actor_name), file=sys.stderr)
    # print(actor_basic, file=sys.stderr)
    # assert actor_obj is not None, "The result of finding the actor was None!"
    return render_template('actor_profile.html', extra=actor_obj, basic=actor_basic)


@app.route('/search_movies', methods=['POST'])
def movie_results():
    movie_name = request.form['movie']
    if movie_name == "undefined":
        return render_template('search.html', message="Did not receive search parameters!")
    else:
        # search the 2 movie SQL databases
        sql1 = "SELECT primaryTitle, runtimeMinutes, genres \
                  FROM movies \
                  WHERE primaryTitle = '" + movie_name + "';"
        sql2 = "SELECT original_title, runtime, tagline, vote_average,  \
               vote_count, spoken_languages, overview, keywords, genres, homepage,  \
               budget, production_companies, revenue, \
               cast, crew \
               FROM movie_500 \
               WHERE original_title = '" + movie_name + "';"
        cursor.execute(sql1)
        movie_result = cursor.fetchall()
        parsed_result = {"genres": []}
        for line in movie_result:
            parsed_result["primaryTitle"] = line[0]
            parsed_result["runtimeMinutes"] = line[1]
            parsed_result["genres"].append(line[2])
        cursor.execute(sql2)
        result_500 = cursor.fetchall()
        print(result_500, file=sys.stderr)
        if result_500:
            result_500 = result_500[0]
        return render_template('movie_detail.html', movie_result=parsed_result, result_500=result_500)



if __name__ == '__main__':
    app.run(host='localhost', port=8080)
