from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from collections import defaultdict

Base = declarative_base()

class Activity(Base):
    __tablename__ = 'viseetor_dwell'

    activity_date = Column(String, primary_key=True)
    gender = Column(String)
    activity = Column(String)

postgres_visee = "postgresql+psycopg2://postgres:Standar123.@viseedb-dev.cbvmxai42qe0.ap-southeast-1.rds.amazonaws.com:5432/main"
engine = create_engine(postgres_visee, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

query = session.query(Activity).order_by(Activity.activity_date).all()

