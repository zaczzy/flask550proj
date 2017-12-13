from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')
@app.route('/result',methods=['POST'])
def search():
    firstLike = request.values.get("firstLike")
    secondLike = request.values.get("secondLike")
    actorLike = request.form['actorLike']
    directorLike = request.form['directorLike']
    actorDislike = request.form['actorDislike']
    directorDislike = request.form['directorDislike']
    return '%s +  %s  + %s + %s + %s + %s' % (firstLike, secondLike, actorLike, directorLike, actorDislike, directorDislike)
if __name__ == '__main__':
    app.run(host = '0.0.0.0', port = 8080)
