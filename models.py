from app import db


class Name(db.Model):
    __tablename__ = 'name'

    nconst = db.Column(db.String, primary_key=True, nullable=False)
    primaryName = db.Column(db.String, nullable=False)
    birthYear = db.Column(db.String, nullable=False)
    deathYear = db.Column(db.String, nullable=False)
    primaryProfession = db.Column(db.String, nullable=False)
    knownForTitles = db.Column(db.String, nullable=False)


class NameTitle(db.Model):
    __tablename__ = 'nameTitle'

    index = db.Column(db.Integer, primary_key=True)
    nconst = db.Column(db.String, db.ForeignKey("name.nconst"))
    name = db.relationship("Name")
    tconst = db.Column(db.String)


class Title(db.Model):
    __tablename__ = 'title'
    tconst = db.Column(db.String, primary_key=True, nullable=False)
    isAdult = db.Column(db.BigInteger, nullable=False)
    startYear = db.Column(db.String, nullable=False)
    endYear = db.Column(db.String, nullable=False)
    runtimeMinutes = db.Column(db.String, nullable=False)
    genres = db.Column(db.String, nullable=False)
    titleType = db.Column(db.String, nullable=False)
    primaryTitle = db.Column(db.String, nullable=False)
    originalTitle = db.Column(db.String, nullable=False)