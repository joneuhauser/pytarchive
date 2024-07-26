from pytarchive.service.db import JsonDatabase


db = JsonDatabase()

alltapes = set(entry["tape"] for entry in db.data if entry.get("tape") is not None)

print(db.format(alltapes))
