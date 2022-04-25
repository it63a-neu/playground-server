from dataclasses import dataclass
from datetime import datetime

from bson import ObjectId
from pymongo.collection import Collection, Mapping

from scripts.database import db

data_fields = (
    'timestamp', 'name', 'gender', 'email',
    'facebook', 'instagram', 'best_action',
    'birthday', 'hobbies', 'slogants',
    'introduction', 'img', 'twitter')

gender_replacement = {
    "Nam": 'male',
    "Nữ": 'female'
}


@dataclass
class Person:
    _id: ObjectId = None
    student_id: str = None
    role: str = None
    timestamp: datetime = None
    name: str = None
    gender: str = None  # male or female
    birthday: datetime = None
    email: str = None
    facebook: str = None
    instagram: str = None
    twitter: str = None
    best_action: str = None
    img: str = None
    hobbies: str = None
    slogants: str = None
    introduction: str = None


class People:
    collection: Collection[Mapping[str, any]]

    def __init__(self):
        self.collection = db['guardians']

    def add_person(self, person: Person):
        self.collection.insert_one(person.__dict__)

    def find_by_name(self, regex: str):
        self.collection.find({"name": {"$regex": f'{regex}'}})

    def fix_genders(self):
        for gender_key in gender_replacement:
            self.collection.update_many(
                {"gender": {"$eq": f'{gender_key}'}},
                {"$set": {"gender": f'{gender_replacement[gender_key]}'}}
            )

    def remove_duplicates(self):
        namelist = set()

        for person in self.collection.find():
            namelist.add(person["name"])

        for name in namelist:
            results = list(self.collection.find({"name": name}))

            if len(results) <= 1:
                continue

            results = list(map(lambda obj: ({"score": len(obj), "data": Person(**obj)}), results))
            results.sort(key=lambda x: x["score"], reverse=True)
            best_result = results[0]
            self.collection.delete_many({"name": {"$eq": f'{name}'}})
            self.collection.insert_one(best_result["data"].__dict__)

    def remove_blanks(self):
        for field in data_fields:
            self.collection.update_many(
                {"$or": [
                    {f"{field}": {"$eq": ''}},
                    {f"{field}": {"$regex": '^[Kk]hông có'}},
                    {f"{field}": None}
                ]},
                {"$unset": {f"{field}": 1}}
            )
