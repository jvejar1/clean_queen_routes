import json
import psycopg2
import sys
import datetime
import time
import array as arr
import numpy as np
import json
import requests

url = 'https://api.routific.com/product/projects'
headers = {'content-type': 'application/json',
            'Authorization': 'bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI1ZTc3ZTdiZjgzYjc5ODAwMTcxZWE1ZjYiLCJpYXQiOjE2MDI1OTM3MTV9.2kTMhN8iUMkSiVwJsJdgtphuEdPVBYvh5eVsX5IOV9k'}

r= requests.get(url, headers=headers)


class Model():
    conn = None
    cursor = None
    def __init__(self):
       
        pass
    def sync_data(self,conn, cursor):

        url = 'https://api.routific.com/product/projects/'
        headers = {'content-type': 'application/json',
        'Authorization': 'bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI1ZTc3ZTdiZjgzYjc5ODAwMTcxZWE1ZjYiLCJpYXQiOjE2MDI1OTM3MTV9.2kTMhN8iUMkSiVwJsJdgtphuEdPVBYvh5eVsX5IOV9k'}
        r = requests.get(url, headers=headers)
        projects = r.json()
        
        for project in projects:
            project_id = project['_id']
            project_name = project['name']
            project_date = project['date']
            project_in_local_db = "select * from projects where id='{}'".format(project_id)
            cursor.execute(project_in_local_db)
            project_in_local_db = cursor.fetchone()
            if project_in_local_db:   
                query = "Update projects set name='{}', project_date='{}' where id='{}'".format(project_name, project_date, project_id)
            else:
                query = "Insert into projects(id, name, project_date) values('{}','{}','{}')".format(project_id, project_name, project_date)
            cursor.execute(query)

        
        #sync the routes for each projects
        for i,_ in enumerate(projects):
            
            project_date = datetime.date.fromisoformat(_['date'])
            is_today_project = project_date == datetime.date.today()
            if not is_today_project:
                continue
            project_name = _['name']
            print('project ',i,'/',len(projects), ': ', project_name)
            project_id = _['_id']
            project_url = 'https://api.routific.com/product/projects/{}'.format(project_id)
            project_request = requests.get(project_url, headers=headers)
            project = project_request.json()
            #register the vehicles
            vehicles_ids = project['fleet'].keys()
            for vehicle_id in vehicles_ids:
                vehicle = project['fleet'][vehicle_id]
    
                vehicle_name = vehicle['name']
                vehicle_shift_start = vehicle['shift-start']
                vehicle_shift_end = vehicle['shift-end']
                vehicle_phone_number = vehicle['phone-number']
                cursor .execute("Select * from vehicles where id='{0}' limit 1;".format(vehicle_id))
                vehicle_already_registered = cursor.fetchone()
                if(vehicle_already_registered):
                    query  = "Update vehicles set name='{}', shift_start='{}', shift_end='{}' where id='{}';".format(vehicle_name,vehicle_shift_start, vehicle_shift_end,vehicle_id)
                    cursor.execute(query)
                else:
                    cursor.execute("Insert into vehicles (id, name, shift_start, shift_end) values ('{}','{}','{}','{}')".format(vehicle_id, vehicle_name,vehicle_shift_start, vehicle_shift_end))
            
            
                cursor.execute(query)
                #print("vehicle ",cursor.statusmessage)

            #check for the cancelled visits are that are in not served
            project['distpatchedSolution']['unserved']
            
            #register the routes
            routes_url = 'https://api.routific.com/product/projects/{}/routes'.format(project_id)
            routes_request = requests.get(routes_url, headers=headers)
            try:
                routes = routes_request.json()        
            except Exception:
                continue
            
            for route in routes:
                vehicle_id = route['vehicle']['id']
                visits = route['solution']['visits']
                #TODO: delete the visits that are no longer in the route

                for visit in visits:
                    is_break = visit['break']
                    arrival_time = visit['arrival_time'] 
                    finish_time = visit['finish_time'] if 'finish_time' in visit else arrival_time #first and last visit(driver) do not have finish_time_field


                    expected_arrival_time = visit['expected_arrival_time'] if 'expected_arrival_time' in visit else arrival_time
                    expected_finish_time = visit['expected_finish_time'] if 'expected_finish_time' in visit else finish_time
                    
                    phone_number = visit['phone'] if 'phone' in visit else None
                    status = visit['status'] if 'status' in visit else None
                    notes = visit['notes'] if 'notes' in visit else None
                    notes2 = visit['notes2'] if 'notes2' in visit else None

                    if 'location' in visit:

                        location_id = visit['location']['id']
                        location_name = visit['location']['name'] if 'name' in visit['location'] else None
                        location_address = visit['location']['address']
                        location_lat = visit['location']['lat']
                        location_long = visit['location']['lng']
                    else:
                        location_id = None
                        location_name = None
                        location_address = None
                        location_lat = None
                        location_long = None

                    #insert or update

                    query = """Select * from visits where location_id = %s and vehicle_id = %s and project_id = %s"""
                    cursor.execute(query, (location_id, vehicle_id, project_id))
                    visit_has_to_be_updated = cursor.fetchone()
                    
                    if(visit_has_to_be_updated):
                        query = """
                        Update visits 
                        set vehicle_id=%s,
                        location_name=%s,
                        location_address=%s,
                        location_long=%s,
                        location_lat=%s,
                        arrival_time=%s,
                        finish_time=%s,
                        estimated_arrival_time=%s,
                        estimated_finish_time =%s,
                        status=%s
                            where location_id=%s;"""
                        args = (vehicle_id,
                        location_name,
                        location_address,
                        location_long,
                        location_lat,
                        arrival_time,
                        finish_time,
                        expected_arrival_time,
                        expected_finish_time,
                        status, 
                        location_id)
                    
                    else:
                        query = """Insert into visits (vehicle_id, project_id, location_id, location_name, location_address, location_long, location_lat, arrival_time, finish_time, estimated_arrival_time, estimated_finish_time,status)
                        values(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                        args = (vehicle_id, project_id, location_id, location_name, location_address, location_long, location_lat, arrival_time, finish_time, expected_arrival_time, expected_finish_time,status)
                    
                    try:
                        cursor.execute(query, args)
                    except psycopg2.errors.DatetimeFieldOverflow as e:
                        conn.rollback()
                        print ("Error while sql: ", cursor.query)
                        print (e)
                    #print('visit ', cursor.statusmessage)
        conn.commit()
        pass

    def get_today_routes(self):
        pass
    def get_incoming_visits(self, route_id):

        pass
    
    def get_today_routes(self):
        pass
    
    def get_planned_visits(self):
        pass
    
    def get_today_projects_ids(self):
        pass

    def out_of_delivery_visits(self):
        today = datetime.datetime.today()
        query = "Select id as k from projects where route_date='{}'".format(today)
        self.cursor.execute(query)
        today_projects_ids = self.cursor.fetchall()
        x=today_projects_ids[0][0]
        today_projects_ids = np.array([i[0] for i in today_projects_ids])
        print (today_projects_ids)
        now_time = datetime.time(11,50)
        
        query = "Select * from visits where project_id in (SELECT id from projects where route_date='{}') and arrival_time>'{}' and status!='done' and status!='arrived' ".format(today, now_time)

        self.cursor.execute(query)
        return self.cursor.fetchall()
        pass

    def get_cancelled_visits(self):
        pass
    
    def get_late_visits(self):
        pass


    def get_today_routes(self, conn, cursor):
        pass
class Notifier():
    def notify(self, order_id):
        pass

#events_file = open('event_jsons.txt') 
conn = None
cursor = None
conn = psycopg2.connect(
    host="localhost",
    database="clean_queen_routes",
    user="postgres",
    password="postgres")
cursor = conn.cursor()

model = Model()

log = open('notifications.log','a+')

while True:
    model.sync_data(conn, cursor)

    #a route is identified by a project_id and a vehicle_id
    today_routes_ids = """
    select v.project_id, v.vehicle_id 
    from visits v, projects p 
    where p.id=v.project_id 
    and p.project_date=%s 
        group by v.project_id, v.vehicle_id"""

    today = datetime.datetime.now().date()
    args = (today,)
    cursor.execute(today_routes_ids, args)     
    routes_ids = cursor.fetchall()
    planned_delivery_notification_window = 90
    for route_id in routes_ids:
        project_id = route_id[0]
        vehicle_id = route_id[1]

        #notify planned delivery
        route_has_been_started_query = "select * from visits where project_id='{}' and vehicle_id='{}' and status='done' order by arrival_time asc limit 1".format(project_id, vehicle_id)
        cursor.execute(route_has_been_started_query)
        route_has_been_started = cursor.fetchone()
        if route_has_been_started:
        #notify the userssssss
            print ('route_started')
            visits_to_notify_query = "select location_name, arrival_time, location_id from visits where project_id='{}' and vehicle_id='{}' and (status!='done' or status is NULL) and (notified_planned_delivery != TRUE or notified_planned_delivery is NULL)".format(project_id, vehicle_id)
            try:
                cursor.execute(visits_to_notify_query)
                visits_to_notify = cursor.fetchall()
            except Exception:
                conn.rollback()
                print("Exception in ", cursor.query)
            
            for visit_to_notify in visits_to_notify:
                visit_arrival_time = visit_to_notify[1]
                visit_id = visit_to_notify[2]
                today = datetime.datetime.today()
                notification_arrival_from = datetime.datetime.combine(today,visit_arrival_time) - datetime.timedelta(minutes = planned_delivery_notification_window)
                notification_arrival_until = datetime.datetime.combine(today,visit_arrival_time) + datetime.timedelta(minutes = planned_delivery_notification_window)
                
                #set the visit as notificated
                visit_was_notified = True
                update_visit_query = "Update visits set notified_planned_delivery=%s where location_id=%s;"
            
                cursor.execute(update_visit_query, (visit_was_notified, visit_id))
                notification = str(datetime.datetime.now())+ ": Notificación pedido "+ str(visit_to_notify[0]) + ": su pedido llegará entre "+str(notification_arrival_from.time()) +" y "+ str(notification_arrival_until.time())
                print ("notification "+ cursor.statusmessage)
                print (notification)
                log.write(notification + "\n")
            conn.commit()
        
        ##notify out of delivery
        window_minutes = 90
        now_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        late_visits_query ="""select * from visits where 
        project_id='{}'
        and vehicle_id='{}'
        and (status is NULL or status!='done')
        and arrival_time <= '{}'
            order by arrival_time asc;
            """.format(project_id,
                        vehicle_id,
                        now_time)
        cursor.execute (late_visits_query)
        late_visits = cursor.fetchall()
        if not late_visits:
            #notify the out of delivery visits

            max_time = datetime.datetime.now() + datetime.timedelta(minutes=window_minutes*.70)
            max_time = max_time.time().strftime("%H:%M:%S")
            out_of_delivery_route ="""select location_name, location_id from visits where 
            project_id='{}'
            and vehicle_id='{}'
            and status is NULL            and arrival_time <= '{}'
            and (notified_out_of_delivery!= TRUE or notified_out_of_delivery is NULL)
                order by arrival_time asc
                """.format(project_id,
                            vehicle_id,
                            max_time)
            cursor.execute (out_of_delivery_route)
            out_of_delivery_visits = cursor.fetchall()
            for visit in out_of_delivery_visits:
                visit_name = visit[0]
                location_id = visit[1]
                notification = "Notificación "+str(visit_name)+ ": Su pedido llegará en los próximos "+ str(window_minutes) + " minutos"
                visit_was_notified = True
                query = "Update visits set notified_out_of_delivery=%s where location_id =%s and vehicle_id =%s and project_id = %s;"
                cursor.execute(query, (visit_was_notified, location_id, vehicle_id, project_id))
                print( notification)
                log.write(notification + "\n")
                pass
            
            conn.commit()
        pass
    
log.close()