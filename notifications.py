import json
import psycopg2
import sys
import datetime
import time
import array as arr
import json
import requests
from psycopg2.extras import NamedTupleCursor
from collections import namedtuple
url = 'https://api.routific.com/product/projects'
headers = {'content-type': 'application/json',
            'Authorization': 'bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI1ZTc3ZTdiZjgzYjc5ODAwMTcxZWE1ZjYiLCJpYXQiOjE2MDI1OTM3MTV9.2kTMhN8iUMkSiVwJsJdgtphuEdPVBYvh5eVsX5IOV9k'}

r= requests.get(url, headers=headers)
class NotificationType:
    CANCELLED = 'cancelled'
    COMPLETED = 'completed'
    SKIPPED = 'skipped'
    OUT_OF_DELIVERY = 'out_of_delivery'
    PLANNED_DELIVERY = 'planned_delivery'

class Model():
    conn = None
    cursor = None
    def __init__(self, conn, cursor):
        self.conn = psycopg2.connect(
            host="localhost",
            database="clean_queen_routes",
            user="postgres",
            password="postgres",
            cursor_factory=NamedTupleCursor)
        self.cursor = self.conn.cursor()

    def get_visit_by_name (self, project, name):
        query = "Select * from visits where location_name like %s and project_id = %s;"
        args = (name, project.id)
        self.cursor.execute(query, args)
        visit = self.cursor.fetchone()
        return visit

    def get_route(self, project_id, vehicle_id):
        Route = namedtuple('route', 'project_id vehicle_id')
        return Route(project_id, vehicle_id)

    def get_project_by_name(self, project_name):
        project_name_like =""
        for word in project_name.split(' '):
            project_name_like+="%{}%".format(word)
        query = "Select * from projects where lower(name) like %s"
        args = (project_name_like,)
        self.cursor.execute(query,args)
        project = self.cursor.fetchone()
        return project
    
    def get_vehicle_by_driver_name(self, driver_name):

        driver_name_like = ""
        for word in driver_name.split(' '):
            driver_name_like += "%{}%".format(word)
        query = "select* from vehicles where lower(name) like %s"
        args = (driver_name_like,)
        self.cursor.execute(query, args)
        vehicle = self.cursor.fetchone()
        return vehicle
    def get_arrival_timestamp(self, visit):
        project = self.get_project(visit)
        timestamp = datetime.datetime.combine(project.project_date, visit.arrival_time)
        return timestamp

    def get_vehicle_by_id(self, vehicle_id):
        query = "Select * from vehicles where id=%s; "
        args = (vehicle_id,)
        self.cursor.execute(query,args)
        vehicle = self.cursor.fetchone()
        return vehicle

    def insert_notification(self, visit, notification_type, arrival_time, expect_from = None, expect_until= None):
        project = self.get_project(visit)
        vehicle = self.get_vehicle(visit)

        if not vehicle:
            vehicle =self.get_last_valid_vehicle_for_cancelled_visit(visit)

        now_datetime = datetime.datetime.now()
        query = """
        Insert into notifications
        (visit_id, vehicle_id, notification_type, arrival_time, expect_from, expect_until, created_at)
        values (%s,%s,%s, %s,%s,%s, %s)
        """
        args = (
            visit.id,
            vehicle.id if vehicle else None,
            notification_type,
            arrival_time,
            expect_from,
            expect_until,
            now_datetime)
        self.cursor.execute(query, args)
        self.conn.commit()


    def get_last_valid_vehicle_for_cancelled_visit(self, visit):
        query = """
        select ve.* 
        from vehicles ve, visit_changes vc 
        where vc.visit_id = %s and vc.old_vehicle_id=ve.id and vc.new_vehicle_id is NULL order by vc.created_at desc limit 1
        """
        args = (visit.id,)
        self.cursor.execute(query, args)
        vehicle = self.cursor.fetchone()
        return vehicle


    def get_notifications(self, visit):
        query = """
        Select * from notifications 
        where visit_id=%s order by created_at asc;
        """
        args = (visit.id,)
        self.cursor.execute(query, args)
        return self.cursor.fetchall()

    def get_last_notification(self, visit):
        query = """
        select * from notifications
        where visit_id = %s order by created_at desc limit 1
        """
        args = (visit.id,)
        self.cursor.execute(query, args)
        last_notification = self.cursor.fetchone()
        return last_notification

    def get_visits(self, route):
        vehicle_id = route.vehicle_id
        project_id = route.project_id
        query = "Select * from visits where vehicle_id = %s and project_id = %s order by arrival_time asc;"
        args = (vehicle_id, project_id)
        self.cursor.execute(query, args)
        visits = self.cursor.fetchall()
        return visits

    def get_visits_without_vehicle_locations(self, route):
        vehicle_id = route.vehicle_id
        project_id = route.project_id
        query = """Select * from visits 
        where vehicle_id = %s and project_id = %s and is_break!=TRUE and is_vehicle_start_location!=TRUE and is_vehicle_end_location!=TRUE
        order by arrival_time asc;
        """
        args = (vehicle_id, project_id)
        self.cursor.execute(query, args)
        visits = self.cursor.fetchall()
        return visits

    def get_project (self, visit):
        project_id = visit.project_id
        query = "Select * from projects where id = %s"
        args = (visit.project_id,)
        self.cursor.execute(query, args)
        project = self.cursor.fetchone()
        return project

    def get_vehicle(self, route):
        vehicle_id = route.vehicle_id
        query = "Select * from vehicles where id = %s"
        args = (vehicle_id,)
        self.cursor.execute(query, args)
        vehicle = self.cursor.fetchone()
        return vehicle

    def get_today_cancelled_visits(self):
        query = """Select v.* from visits v, projects p 
        where v.project_id = p.id and p.project_date = %s
        and vehicle_id is NULL and is_break!=TRUE and is_vehicle_start_location!=TRUE and is_vehicle_end_location!=TRUE
        """
        today = datetime.datetime.now().date()
        self.cursor.execute(query, (today,))
        cancelled_visits = self.cursor.fetchall()
        return cancelled_visits

    def get_today_routes(self):
        today_routes_ids = """
        select v.project_id, v.vehicle_id 
        from visits v, projects p 
        where p.id=v.project_id 
        and p.project_date=%s and v.vehicle_id is not null
            group by v.project_id, v.vehicle_id"""

        today = datetime.datetime.now().date()
        args = (today,)
        self.cursor.execute(today_routes_ids, args)     
        routes_ids = self.cursor.fetchall()
        return routes_ids
    
    
    def get_routes(self, project_name):
        routes_query = """
        select v.project_id, v.vehicle_id 
        from visits v, projects p 
        where p.id=v.project_id 
        and p.name=%s and v.vehicle_id is not null
            group by v.project_id, v.vehicle_id"""

        args = (project_name,)
        self.cursor.execute(routes_query, args)
        routes = self.cursor.fetchall()
        return routes
    

    def route_is_late(self, route):
        project_id = route.project_id
        vehicle_id = route.vehicle_id
        allowed_lateness_minutes = 5

        critical_datetime = datetime.datetime.now() - datetime.timedelta(minutes = allowed_lateness_minutes)
        late_visits_query ="""select * from visits where 
            project_id='{}'
            and vehicle_id='{}'
            and (status is NULL or status!='done')
            and arrival_time <= '{}'
                order by arrival_time asc;
                """.format(project_id,
                            vehicle_id,
                            critical_datetime.time())
        self.cursor.execute (late_visits_query)
        late_visits = self.cursor.fetchall()

        if late_visits:
            return True
        else:
            return False
    
    def route_has_been_started(self, route_id):
        project_id = route_id.project_id
        vehicle_id = route_id.vehicle_id
        query = "select * from visits where project_id='{}' and vehicle_id='{}' and is_vehicle_start_location =TRUE and status='done' order by arrival_time asc limit 1".format(project_id, vehicle_id)
        self.cursor.execute(query)
        route_has_been_started = self.cursor.fetchone()
        if (route_has_been_started):
            return True
        else:
            return False
            

    def parse_and_limit_time(self, time_str):
        max_time = datetime.time(hour = 23, minute= 59)
        if time_str:
            try:
                parsed_time = datetime.datetime.strptime(time_str, "%H:%M").time()
            except ValueError as e:
                parsed_time = max_time
            return parsed_time
    

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

        conn.commit()
        
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
                vehicle_phone_number = vehicle['phone-number'] if 'phone-number' in vehicle else None
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
                    
                    #limit the time if is more than 23:59
                    max_time = datetime.time.max
                    
                    arrival_time = self.parse_and_limit_time(arrival_time)
                    finish_time = self.parse_and_limit_time(finish_time)
                    expected_arrival_time = self.parse_and_limit_time(expected_arrival_time)
                    expected_finish_time = self.parse_and_limit_time(expected_finish_time)
                    
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
                    visit_id = location_id
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

                unserved_visits_query = "update visits set vehicle_id=NULL where location_id not in %s and project_id=%s and vehicle_id=%s returning id"
                cursor.execute(unserved_visits_query, (tuple(ids_of_served_visits), project_id, vehicle_id))
                if cursor.rowcount>0:
                    print ("Visits that are no longer with {} ({}): {}".format(vehicle_name, project_name, cursor.rowcount))
        
                unserved_visits = cursor.fetchall()
                visit_change_query = "Insert into visit_changes (visit_id, old_vehicle_id, new_vehicle_id) values (%s,%s,%s)"
                args = map(lambda visit: (visit.id, vehicle_id, None), unserved_visits)         
                cursor.executemany(visit_change_query, args)
                
                if cursor.rowcount>0:
                    print ("Visits that are no longer with {} ({}): {}".format(vehicle_name, project_name, cursor.rowcount))
                
        conn.commit()
        pass

#events_file = open('event_jsons.txt') 

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
    model = Model(conn=conn, cursor = cursor)

    while True:
        model.sync_data(conn, cursor)
        log_lines = ""
        #a route is identified by a project_id and a vehicle_id
        
        today_routes = today_routes = model.get_today_routes()
        now_dt = datetime.datetime.now()
        
        planned_delivery_notification_window = 70
        out_of_delivery_minutes_bound = 30
        early_visits_dt_bound = now_dt + datetime.timedelta(minutes = out_of_delivery_minutes_bound)
        early_visits_time_bound = early_visits_dt_bound.time()
                    
        for route in today_routes:
            vehicle_id = route.vehicle_id
            project_id = route.project_id

            route_has_been_started = model.route_has_been_started(route)
            route_is_late = model.route_is_late(route)

            visits = model.get_visits_without_vehicle_locations(route)

            for visit in visits:
                
                #notify planned delivery
                now_time =  now_dt.time()
                last_notification = model.get_last_notification(visit)
                visit_is_not_completed_nor_skipped = visit.status != 'done' and visit.status !='skipped'
                
                visit_are_late_for_customer= last_notification and (last_notification.notification_type == NotificationType.PLANNED_DELIVERY or last_notification.notification_type == NotificationType.OUT_OF_DELIVERY) and last_notification.expect_until < now_time

                #check if is out of delivery or an early visit
                out_of_delivery_dt_bound = now_dt + datetime.timedelta(minutes =out_of_delivery_minutes_bound * 0.7)
                is_out_of_delivery = visit.arrival_time < out_of_delivery_dt_bound.time()
                if (not route_is_late) and route_has_been_started and visit_is_not_completed_nor_skipped and is_out_of_delivery and ((not last_notification) or last_notification.notification_type == NotificationType.PLANNED_DELIVERY or last_notification.notification_type == NotificationType.CANCELLED):
                    expect_from = now_dt
                    expect_until = now_dt + datetime.timedelta(minutes = out_of_delivery_minutes_bound)
                    
                    model.insert_notification(
                        visit = visit,
                        notification_type = NotificationType.OUT_OF_DELIVERY,
                        arrival_time = visit.arrival_time,
                        expect_from=expect_from.time(),
                        expect_until = expect_until.time()
                        )
                    pass
                
                #check if is planned_delivery
                last_notification = model.get_last_notification(visit)
                visit_is_in_the_future = visit.arrival_time > now_time
                if route_has_been_started and (not route_is_late) and visit_is_in_the_future and visit_is_not_completed_nor_skipped and ((not last_notification) or last_notification.notification_type == NotificationType.CANCELLED):
                    expect_from = datetime.datetime.combine(now_dt,visit.arrival_time) - datetime.timedelta(minutes = planned_delivery_notification_window)
                    expect_from = max(expect_from, now_dt + datetime.timedelta(minutes=7))

                    expect_until = datetime.datetime.combine(now_dt,visit.arrival_time) + datetime.timedelta(minutes = planned_delivery_notification_window)
                    
                    model.insert_notification(
                        visit = visit,
                        notification_type = NotificationType.PLANNED_DELIVERY,
                        arrival_time = visit.arrival_time,
                        expect_from=expect_from.time(),
                        expect_until = expect_until.time()
                        )
                    
                #notification completed
                last_notification = model.get_last_notification(visit)
                visit_is_completed = visit.status == 'done'
                if visit_is_completed and ((not last_notification) or last_notification.notification_type != NotificationType.COMPLETED):
                    model.insert_notification(
                        visit= visit,
                        notification_type = NotificationType.COMPLETED,
                        arrival_time = visit.arrival_time,
                        expect_from= None,
                        expect_until = None
                        )
                    pass

                #visit was skipped
                last_notification = model.get_last_notification(visit)
                visit_was_skipped  = visit.status == 'skipped'
                if visit_was_skipped and ((not last_notification) or last_notification.notification_type != NotificationType.SKIPPED ):
                    model.insert_notification(
                        visit = visit,
                        notification_type = NotificationType.SKIPPED,
                        arrival_time = visit.arrival_time,
                        expect_from = None,
                        expect_until = None
                    )
                
                
        #TODO: process today cancelled routes  
        cancelled_visits = model.get_today_cancelled_visits()
        for visit in cancelled_visits:
            last_notification = model.get_last_notification(visit)
            if last_notification and (last_notification.notification_type == NotificationType.PLANNED_DELIVERY or last_notification.notification_type == NotificationType.OUT_OF_DELIVERY):
                model.insert_notification(
                        visit = visit,
                        notification_type = NotificationType.CANCELLED,
                        arrival_time = visit.arrival_time,
                        expect_from= None,
                        expect_until = None
                        )
                
                pass
        continue
    
    