from notifications import Model
from tabulate import tabulate
import sys
if __name__=="__main__":
    model = Model(None, None)
    project_name = sys.argv[1]

    visit_name = sys.argv[2]
    
    project = model.get_project_by_name(project_name)
    visit= model.get_visit_by_name (project, visit_name)
    notifications = model.get_notifications(visit)
    #the table
    headers =['Created_at', 'arr_time', 'vehic_name', 'ntf_type']
    notifications = map(lambda ntf: (
        ntf.created_at.time(), 
        ntf.arrival_time, 
        model.get_vehicle_by_id(ntf.vehicle_id).name if model.get_vehicle_by_id(ntf.vehicle_id) else '',
        ntf.notification_type 
        ), notifications)
    
    print (tabulate(notifications, headers))
    print ("-"*10)