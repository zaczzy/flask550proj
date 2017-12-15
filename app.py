from flask import Flask, render_template, request
import pymongo as mongo
import sys
mongo_client = mongo.MongoClient(
    "mongodb://cis550mongo:cis550project@mongocluster-shard-00-00-5qypc.mongodb.net:27017,"
    "mongocluster-shard-00-01-5qypc.mongodb.net:27017,"
    "mongocluster-shard-00-02-5qypc.mongodb.net:27017/test?ssl=true&replicaSet=mongocluster-shard-0&authSource=admin")
actors_db = mongo_client.actors
app = Flask(__name__)
male_actors = actors_db.male
female_actors = actors_db.female


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/result', methods=['POST'])
def search():
    # firstLike = request.values.get("firstLike")
    firstLike = request.form['firstLike']
    # secondLike = request.values.get("secondLike")
    secondLike = request.form['secondLike']
    actorLike = request.form['actorLike']
    directorLike = request.form['directorLike']
    actorDislike = request.form['actorDislike']
    directorDislike = request.form['directorDislike']
    return '%s +  %s  + %s + %s + %s + %s' % (
        firstLike, secondLike, actorLike, directorLike, actorDislike, directorDislike)


@app.route('/search_actors')
def actor_search():
    return render_template('search_actors.html')


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
    assert actor_name != "undefined" and actor_sex is not None, "Did not receive search parameters!"
    if actor_sex == "male":
        actor_obj = male_actors.find_one({"_id": hash_name(actor_name)})
    elif actor_sex == "female":
        actor_obj = female_actors.find_one({"_id": hash_name(actor_name)})
    else:
        actor_obj = None
    # print(actor_obj, file=sys.stderr)
    # assert actor_obj is not None, "The result of finding the actor was None!"
    return render_template('actor_profile.html', actor=actor_obj)


if __name__ == '__main__':
    app.run(host='localhost', port=8080)
