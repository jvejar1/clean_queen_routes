import psycopg2
from psycopg.extras import NamedTupleCursor
if __name__ == "__main__":
    conn = None
    cursor = None
    conn = psycopg2.connect(
    host="localhost",
    database="clean_queen_routes",
    user="postgres",
    password="postgres",
    cursor_factory=NamedTupleCursor)
    cursor = conn.cursor()

    project_name_arg = "prueba 10"
    project_name_like =""
    for word in project_name_arg.split(' '):
        project_name_like+="%{}%".format(word)
    
    driver_name_arg = "esteban"
    driver_name_like = ""
    for word in driver_name_arg.split(' '):
        driver_name_like += "%{}%".format(word)

    #the project
    query = "Select * from projects lower(p.name) like %s"
    args = (project_name_like,)
    cursor.execute(query,args)
    project =cursor.fetchone()

    #the driver
    query = "select* from vehicles where name like %s"
    args = (driver_name_like,)
    cursor.execute(query, args)
    vehicle = cursor.fetchone()

    visits = "select * from visits where (project_id = %s and vehicle_id =%s)"