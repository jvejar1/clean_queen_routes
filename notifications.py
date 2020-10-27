import json
import psycopg2
import sys
import datetime
import time
import array as arr
import numpy as np
import json
import requests
from psycopg2.extras import NamedTupleCursor
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
                start_location = vehicle['start-location']
                start_location_id = start_location['id']
                start_location_address = start_location['address']
                start_location_lat = start_location['lat']
                start_location_long = start_location['lng']

                end_location = vehicle['end-location']
                end_location_id = None
                end_location_address = None
                end_location_lat = None
                end_location_long = None
                if end_location:
                    end_location_id = end_location['id']
                    end_location_address = end_location['address']
                    end_location_lat = end_location['lat']
                    end_location_long = end_location['lng']

                cursor .execute("Select * from vehicles where id='{0}' limit 1;".format(vehicle_id))
                vehicle_already_registered = cursor.fetchone()
                if(vehicle_already_registered):
                    query  = "Update vehicles set name=%s, shift_start=%s, shift_end=%s, start_location_id=%s, end_location_id=%s where id=%s;"
                    args = (vehicle_name, vehicle_shift_start, vehicle_shift_end, start_location_id, end_location_id, vehicle_id)
                    cursor.execute(query, args)
                else:
                    query ="Insert into vehicles (id, name, shift_start, shift_end, start_location_id, end_location_id) values (%s,%s,%s,%s, %s, %s)"
                    args = (vehicle_id, vehicle_name,vehicle_shift_start, vehicle_shift_end, start_location_id, end_location_id)
                    cursor.execute(query, args)
                
                #print("vehicle ",cursor.statusmessage)

            #check for the cancelled visits are that are in not served  
            
            
            #register the routes
            routes_url = 'https://api.routific.com/product/projects/{}/routes'.format(project_id)
            routes_request = requests.get(routes_url, headers=headers)
            try:
                routes = routes_request.json()        
            except Exception:
                continue
            fleet = project['fleet']
            for route in routes:
                vehicle_id = route['vehicle']['id']
                vehicle = fleet[vehicle_id]
                visits = route['solution']['visits']
                ids_of_served_visits =[]
                for visit in visits:
                    is_break = visit['break']
                    arrival_time = visit['arrival_time'] 
                    finish_time = visit['finish_time'] if 'finish_time' in visit else arrival_time #first and last visit(driver) do not 
                    expected_arrival_time = visit['expected_arrival_time'] if 'expected_arrival_time' in visit else arrival_time
                    expected_finish_time = visit['expected_finish_time'] if 'expected_finish_time' in visit else finish_time
                    phone_number = visit['phone'] if 'phone' in visit else None
                    status = visit['status'] if 'status' in visit else None
                    notes = visit['notes'] if 'notes' in visit else None
                    notes2 = visit['notes2'] if 'notes2' in visit else None

                    if 'location' in visit:

                        location_id = visit['location']['id']
                        location_name = visit['location']['name'] if 'name' in visit['location'] else ''
                        location_address = visit['location']['address']
                        location_lat = visit['location']['lat']
                        location_long = visit['location']['lng']
                    else:
                        #is a break
                        location_id = location_name = location_address =''
                        location_lat = location_long = None

                    is_vehicle_start_location = location_id == vehicle['start-location']['id']
                    is_vehicle_end_location = False if vehicle['end-location']==None else location_id == vehicle['end-location']['id']

                    #insert or update: the visit can be in the project but in another vehicle
                    query = """Select * from visits 
                    where (location_id = %s and project_id = %s) and (is_break=FALSE or (is_break=TRUE and vehicle_id=%s))"""
                    cursor.execute(query,(location_id, project_id, vehicle_id))
                    if cursor.rowcount>0:
                        query = """Update visits set vehicle_id=%s, location_name=%s, location_address=%s, location_long=%s,
                        location_lat=%s, arrival_time=%s, finish_time=%s, estimated_arrival_time=%s, estimated_finish_time =%s,
                        status=%s, is_vehicle_start_location=%s, is_vehicle_end_location=%s where location_id=%s and project_id=%s;"""
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
                        is_vehicle_start_location,
                        is_vehicle_end_location,
                        location_id,
                        project_id)

                    else:
                        #insert
                        query = """Insert into visits (vehicle_id, project_id, location_id, location_name, location_address, location_long, location_lat, arrival_time, finish_time, estimated_arrival_time, estimated_finish_time,status, is_break, is_vehicle_start_location, is_vehicle_end_location)
                        values(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                        args = (vehicle_id, project_id, location_id, location_name, location_address, location_long, location_lat, arrival_time, finish_time, expected_arrival_time, expected_finish_time,status, is_break, is_vehicle_start_location, is_vehicle_end_location)

                    try:
                        cursor.execute(query, args)
                    except psycopg2.errors.DatetimeFieldOverflow as e:
                        conn.rollback()
                        print ("Error while sql: ", cursor.query)
                        print (e)
                    #print('visit ', cursor.statusmessage)
                    ids_of_served_visits.append(location_id)

                unserved_visits_query = "update visits set vehicle_id=NULL where location_id not in %s and project_id=%s and vehicle_id=%s"
                cursor.execute(unserved_visits_query, (tuple(ids_of_served_visits), project_id, vehicle_id))
                if cursor.rowcount>0:
                    print ("Visits that are no longer with {} ({}): {}".format(vehicle_name, project_name, cursor.rowcount))
                
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
    password="postgres",
    cursor_factory=NamedTupleCursor)
cursor = conn.cursor()

model = Model()

while True:
    model.sync_data(conn, cursor)

    log_lines = ""
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
    early_visits_time_until = datetime.datetime.now() + datetime.timedelta(minutes = planned_delivery_notification_window)
    early_visits_time_until = early_visits_time_until.time()
    now_time = datetime.datetime.now().time()

    for route_id in routes_ids:
        project_id = route_id[0]
        vehicle_id = route_id[1]
        #notify planned delivery
        route_has_been_started_query = "select * from visits where project_id='{}' and vehicle_id='{}' and status='done' order by arrival_time asc limit 1".format(project_id, vehicle_id)
        cursor.execute(route_has_been_started_query)
        route_has_been_started = cursor.fetchone()
        if route_has_been_started:
        #notify the userssssss
            visits_to_notify_query = """select location_name, arrival_time, location_id from visits vi, vehicles ve
            where vi.project_id = %s 
            and vi.vehicle_id=%s 
            and (vi.status!='done' or vi.status is NULL) 
            and (vi.notified_planned_delivery != TRUE or vi.notified_planned_delivery is NULL)
            and ve.id = vi.vehicle_id and vi.location_id != ve.start_location_id and vi.location_id!=ve.end_location_id
            and vi.is_break!=TRUE and vi.arrival_time >= %s
                order by arrival_time asc;
            """
            args = (project_id, vehicle_id, early_visits_time_until)

            try:
                cursor.execute(visits_to_notify_query, args)
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
                update_visit_query = "Update visits set notified_planned_delivery=%s where location_id=%s and project_id=%s;"            
                cursor.execute(update_visit_query, (visit_was_notified, visit_id, project_id))

                notification = str(datetime.datetime.now().time())+ " \t "+ (visit_to_notify[0]) + "\t Ha salido un vehículo con su pedido y llegará llegará entre "+str(notification_arrival_from.time()) +" y "+ str(notification_arrival_until.time())
                print ("notification "+ cursor.statusmessage)
                print (notification)
                log_lines += notification + "\n"
            
            early_visits_to_notify_query = """ select location_name, arrival_time, location_id from visits vi, vehicles ve
            where vi.project_id=%s 
            and vi.vehicle_id=%s 
            and (vi.status!='done' or vi.status is NULL) 
            and (vi.notified_planned_delivery != TRUE or vi.notified_planned_delivery is NULL)
            and ve.id = vi.vehicle_id and vi.location_id != ve.start_location_id and vi.location_id!=ve.end_location_id
            and vi.is_break!=TRUE and vi.arrival_time <=%s
                order by arrival_time asc"""
            args = (project_id, vehicle_id, early_visits_time_until)
            cursor.execute(early_visits_to_notify_query, args)
            early_visits = cursor.fetchall()
            for visit in early_visits:
                location_name = visit.location_name
                location_id = visit.location_id
                notification = str(datetime.datetime.now().time())+ " \t {} \t Ha salido un vehículo con su pedido y llegará pronto".format(location_name)

                #update the visit
                visit_was_notified = True
                update_query = "Update visits set notified_out_of_delivery=%s, notified_planned_delivery=%s where location_id = %s and project_id=%s"
                cursor.execute(update_query, (visit_was_notified, visit_was_notified, location_id, project_id))
                print (notification)
                print ("notification "+ cursor.statusmessage)
                log_lines += notification + "\n"
                #notify

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
            and status is NULL and arrival_time <= '{}'
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
                notification = str(datetime.datetime.now().time())+ "\t"+visit_name +"\tSu pedido llegará en los próximos "+ str(window_minutes) + " minutos"
                visit_was_notified = True
                query = "Update visits set notified_out_of_delivery=%s where location_id =%s and vehicle_id =%s and project_id = %s;"
                cursor.execute(query, (visit_was_notified, location_id, vehicle_id, project_id))
                print( notification)
                log_lines += notification + "\n"
            
            conn.commit()
        pass

        #notify the completed visits
        completed_visits_query = """Select location_name, vi.phone_number as phone, location_id, status from visits vi, vehicles ve 
        where vi.project_id=%s and vi.vehicle_id=%s and (vi.status='done' or vi.status='skipped')
        and vi.vehicle_id=ve.id and vi.location_id!=ve.start_location_id and vi.location_id!=ve.end_location_id and vi.is_break!=TRUE
        and (notified_completed!=TRUE or notified_completed is NULL) """
        args = (project_id, vehicle_id)
        cursor.execute(completed_visits_query, args)
        completed_visits = cursor.fetchall()
        if cursor.rowcount>0:
            print ("Notifying completed")
        for visit in completed_visits:
            location_name = visit.location_name
            phone = visit.phone
            location_id = visit.location_id
            status = visit.status
            visit_was_notified = True
            update_query = "Update visits set notified_completed=%s where project_id = %s and vehicle_id=%s and location_id=%s"
            args = (visit_was_notified, project_id, vehicle_id, location_id)
            cursor.execute(update_query, args)

            notification = str(datetime.datetime.now().time()) + " \t "+location_name+" \t Su pedido ha sido "+ status
            print (notification)
            print ("notification completed "+ cursor.statusmessage)
            log_lines += notification + "\n"

        ##notify cancelled
        cancelled_visits_query = """Select * from visits where
        vehicle_id is null and project_id=%s 
        and is_vehicle_start_location = FALSE and is_vehicle_end_location=FALSE and is_break = FALSE 
        and (notified_cancelled = FALSE or notified_cancelled is NULL) and (notified_planned_delivery = TRUE)"""
        args = (project_id,)
        cursor.execute(cancelled_visits_query, args)
        cancelled_visits = cursor.fetchall()
        for visit in cancelled_visits:
            visit_name = visit.location_name
            location_id = visit.location_id
            visit_was_notified = True
            update_query ="Update visits set notified_cancelled=%s where project_id=%s and location_id= %s"
            args = (visit_was_notified, project_id, location_id)
            cursor.execute(update_query, args)

            notification = "{} \t {} \t Su pedido ha sido cancelado".format(datetime.datetime.now().time(), visit_name )
            print (notification)
            log_lines +=notification + "\n"
    
    conn.commit()
    
    log = open('notifications.log', 'a')
    log.write(log_lines)
    log.close()