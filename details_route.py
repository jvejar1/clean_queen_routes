import psycopg2
from psycopg2.extras import NamedTupleCursor
from notifications import Model
from tabulate import tabulate
import sys
if __name__ == "__main__":
    
    model = Model(None, None)

    project_name_arg = sys.argv[1]
    driver_name_arg = sys.argv[2]
    
    project = model.get_project_by_name(project_name_arg)
    vehicle = model.get_vehicle_by_driver_name(driver_name_arg)
    route = model.get_route(project.id, vehicle.id)

    visits = model.get_visits(route)
    headers = ['Name', 'arr_time', 'ntf_count', 'Last_notf', 'Status' ]
    table = map(lambda visit: (
        visits.index(visit) +1, 
        visit.location_name, 
        visit.arrival_time, 
        len(model.get_notifications(visit)),
        model.get_last_notification(visit).notification_type if model.get_last_notification(visit) else '',
        visit.status
        ),
         visits)
    print(tabulate(table, headers))
    print("-"*15)