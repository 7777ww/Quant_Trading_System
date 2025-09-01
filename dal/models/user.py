# models.py
from sqlalchemy import Column, Integer, String
from .base_model import Base



class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    email = Column(String(120))

