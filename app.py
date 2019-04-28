import json

from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy

from config import Config
from helper import AlchemyEncoder

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = Config.DATABASE_URI
db = SQLAlchemy(app)

from models import Name, Title, NameTitle

PER_PAGE = 10


@app.route('/')
def index():
    start_year = request.args.get('startYear', '')
    person = request.args.get('person', '')
    page = request.args.get('page', 0)
    genre = request.args.get('genre', '')
    if person:
        name = db.session.query(Title.originalTitle, Name.primaryName) \
            .join(NameTitle, NameTitle.tconst == Title.tconst) \
            .join(Name, Name.nconst == NameTitle.nconst) \
            .filter(Name.primaryName.contains(person)) \
            .group_by(Title.originalTitle, Name.primaryName).limit(PER_PAGE).offset(page).all()
    else:
        name = db.session.query(Title.originalTitle, Name.primaryName)\
            .join(NameTitle, NameTitle.tconst == Title.tconst)\
            .join(Name, Name.nconst == NameTitle.nconst) \
            .filter(Title.startYear.contains(start_year)) \
            .filter(Title.genres.contains(genre)) \
            .group_by(Title.originalTitle, Name.primaryName).limit(PER_PAGE).offset(page).all()

    return json.dumps(name, cls=AlchemyEncoder)
