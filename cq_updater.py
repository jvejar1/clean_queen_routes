from notifications import Model
import cq_util
import psycopg2
from psycopg2.extras import NamedTupleCursor
class Order_Status():
    AWAITING_SHIPMENT = "awaiting-shipment"
    SHIPPED = "shipped"
    COMPLETED = "completed"

class ECommerceSystems():
    CLEAN_QUEEN = "clean queen"

class OrdersModel():       
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database="clean_queen_routes",
            user="postgres",
            password="postgres",
            cursor_factory=NamedTupleCursor)
        self.cursor = self.conn.cursor()

    def sync_remote_order(self, remote_id):
        pass

    def identify_ecommerce_for_order(self, remote_remote_order_id):
        is_cq = cq_util.check_if_order_exists_remote(remote_remote_order_id)
        return ECommerceSystems.CLEAN_QUEEN if is_cq else None

    def find_order_by_remote_id(self, remote_remote_order_id):
        query = "Select * from orders where remote_id = %s"
        args = (remote_remote_order_id,)
        self.cursor.execute(query,args)
        self.conn.commit()
        return self.cursor.fetchone()

    def order_is_from_clean_queen(self, order):
        return order.ecommerce == ECommerceSystems.CLEAN_QUEEN

    def insert_order(self, remote_id, ecommerce, eta = None, status = None):
        query = "Insert into orders (remote_id, ecommerce, eta, status) values (%s, %s, %s, %s) returning id, remote_id, ecommerce, eta, status"
        params = (remote_id, ecommerce, eta, status)
        self.cursor.execute(query, params)
        self.conn.commit()

        return self.cursor.fetchone()

    def update_order(self, local_order_id, eta, status):
        query = "Update orders set eta =%s, status =%s where id =%s returning *"
        args = (eta, status, local_order_id)
        self.cursor.execute(query, args)
        self.conn.commit()
        return self.cursor.fetchone()

if __name__=="__main__":
    model = Model(None, None)
    orders_model = OrdersModel()
    while True:
        routes = model.get_routes(project_name = "prueba 12")
        for route in routes:
            visits = model.get_visits_without_vehicle_locations(route)
            for visit in visits:
                eta = model.get_arrival_timestamp(visit)
                route_has_been_started = model.route_has_been_started(route)
                visit_was_completed = visit.status == 'done'
                visit_was_skipped = visit.status == 'skipped'
                visit_was_shipped = route_has_been_started and (not visit_was_completed) and (not visit_was_skipped)
                
                remote_orders_ids = visit.location_name.split('|')

                for remote_order_id in remote_orders_ids: 
                    order = orders_model.find_order_by_remote_id(remote_order_id)
                    if not order:
                        ecommerce = orders_model.identify_ecommerce_for_order(remote_order_id)
                        order = orders_model.insert_order(remote_order_id, ecommerce)

                    if orders_model.order_is_from_clean_queen(order):
                        order_final_status = None
                        if visit_was_shipped:
                            order_final_status = Order_Status.SHIPPED
                        elif visit_was_skipped:
                            order_final_status = Order_Status.AWAITING_SHIPMENT
                        elif visit_was_completed:
                            order_final_status = Order_Status.COMPLETED
                        else:
                            order_final_status = Order_Status.AWAITING_SHIPMENT

                        needs_to_update_order = order_final_status != order.status or eta != order.eta
                       
                        if needs_to_update_order:
                            #update remote
                            cq_util.update_order(remote_order_id, order_final_status, eta.isoformat())
                            #update local
                            orders_model.update_order(order.id, eta, order_final_status)                        
                    
                    
        cancelled_visits = model.get_today_cancelled_visits()
        for visit in cancelled_visits:
        
            eta = model.get_arrival_timestamp(visit)
        
            remote_orders_ids = visit.location_name.split('|')

            for remote_order_id in remote_orders_ids: 
                order = orders_model.find_order_by_remote_id(remote_order_id)
                if not order:
                    ecommerce = orders_model.identify_ecommerce_for_order(remote_order_id)
                    order = orders_model.insert_order(remote_order_id, ecommerce)

                if orders_model.order_is_from_clean_queen(order):
                    order_final_status = Order_Status.AWAITING_SHIPMENT

                    needs_to_update_order = order_final_status != order.status or eta != order.eta
                    #update remote
                    if needs_to_update_order:
                        cq_util.update_order(remote_order_id, order_final_status, eta.isoformat())
                        orders_model.update_order(order.id, eta.isoformat(), order_final_status)                        
                    #update locally based in the result of query :s
      
