from notifications import Model
import psycopg2
from psycopg2.extras import NamedTupleCursor
import datetime
conn = psycopg2.connect(
    host="localhost",
    database="clean_queen_routes",
    user="postgres",
    password="postgres",
    cursor_factory=NamedTupleCursor)
cursor = conn.cursor()
model = Model()

model.conn = conn
model.cursor = cursor

#model.insert_notification("a", "a", "cancelled", datetime.datetime.now())

print(model.get_notifications(None, None))