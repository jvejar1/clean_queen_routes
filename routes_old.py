import datetime
import json
import psycopg2
import sys

#receive json
visit_status_update_event = '{"user":"ricardo@cleanqueen.cl","event":"visit status update","createdAt":"2020-04-17T09:36:16.091Z","visit":{"log":[{"status":"arrived","date":1587116169837,"hasLiveTracking":true},{"status":"done","date":1587116171690,"hasLiveTracking":true}],"break":false,"arrival_time":"05:34","finish_time":"05:36","idle_time":0,"start_time":"05:28","location":{"id":"9de52190a413483dfa572313927af794","name":"15637","address":"Las Encinas 5759, Vitacura","lat":-33.3841784,"lng":-70.58600779999999},"phone":"992394794","notes":"Alejandra Moya","original_start_time":"11:08","original_finish_time":"11:16","logged_arrival_time":1587116169837,"hasLiveTracking":true,"status":"done","expected_arrival_time":"05:40","expected_finish_time":"05:48"},"vehicle":{"start-location":{"coords":{"lat":-33.4009489,"lng":-70.52161339999999},"id":"06e1e91b7fc94415a2560a9884fcf141","address":"Camino La Fuente 1592, Las Condes, Región Metropolitana, Chile","lat":-33.4009489,"lng":-70.52161339999999},"end-location":{"coords":{"lat":-33.4009489,"lng":-70.52161339999999},"id":"6730387786f645aac7eb8450b2bf9e13","address":"Camino La Fuente 1592, Las Condes, Región Metropolitana, Chile","lat":-33.4009489,"lng":-70.52161339999999},"hidden":false,"types":[],"_id":"5e8c8568a0142500183535e3","name":"Carlos HZGF30","shift-start":"10:00","shift-end":"14:30","phone-number":"+56985015352","capacity":null,"breaks":[],"id":"3cc122c2e34a4782b4167de5b69511dc","_user":"5e77e7bf83b79800171ea5f6","__v":0,"routeStatus":[{"log":[{"status":"done","date":1587114972262,"hasLiveTracking":true}],"break":false,"arrival_time":"05:16","idle_time":0,"start_time":"05:16","location":{"id":"06e1e91b7fc94415a2560a9884fcf141","name":"Carlos HZGF30","address":"Camino La Fuente 1592, Las Condes, Región Metropolitana, Chile","lat":-33.4009489,"lng":-70.52161339999999},"hasLiveTracking":true,"status":"done","original_start_time":"10:00","expected_arrival_time":"10:00"},{"log":[{"status":"arrived","date":1587114976123,"hasLiveTracking":true},{"status":"done","date":1587114977716,"hasLiveTracking":true}],"break":false,"arrival_time":"05:16","finish_time":"05:16","idle_time":0,"start_time":"05:08","location":{"id":"1f0260a5764847d8e0b5dbd1648b7425","name":"15644","address":"Las hualtatas 11.573, Vitacura","lat":-33.3742789,"lng":-70.526083},"phone":"982397702","notes":"Constanza Gómez","notes2":"Pasaje sin salida","original_start_time":"10:08","original_finish_time":"10:16","logged_arrival_time":1587114976123,"hasLiveTracking":true,"status":"done","expected_arrival_time":"05:24","expected_finish_time":"05:32"},{"log":[{"status":"arrived","date":1587115511395,"hasLiveTracking":true},{"status":"done","date":1587115513183,"hasLiveTracking":true}],"break":false,"arrival_time":"05:17","finish_time":"05:25","idle_time":0,"start_time":"05:17","location":{"id":"999f28f7614040e0d56473c50ad28290","name":"15628","address":"Av. Los Litres 1750, Lo Barnechea","lat":-33.3164552,"lng":-70.5498312},"phone":"997293899","notes":"Antonieta Baeza","original_start_time":"10:30","original_finish_time":"10:38","logged_arrival_time":1587115511395,"hasLiveTracking":true,"status":"done","expected_arrival_time":"05:30","expected_finish_time":"05:38"},{"log":[{"status":"arrived","date":1587116058594,"hasLiveTracking":true},{"status":"done","date":1587116060187,"hasLiveTracking":true}],"break":false,"arrival_time":"05:26","finish_time":"05:34","idle_time":0,"start_time":"05:26","location":{"id":"f1be1a253e8c425087d7be382fe56143","name":"15632","address":"Camino del Condor 7899, Vitacura","lat":-33.3672289,"lng":-70.5656364},"phone":"981381798","notes":"Amalia Diuana","notes2":"calle mal enumerada, pasar el 7941","original_start_time":"10:53","original_finish_time":"11:01","logged_arrival_time":1587116058594,"hasLiveTracking":true,"status":"done","expected_arrival_time":"05:39","expected_finish_time":"05:47"},{"log":[{"status":"arrived","date":1587116169837,"hasLiveTracking":true},{"status":"done","date":1587116171690,"hasLiveTracking":true}],"break":false,"arrival_time":"05:34","finish_time":"05:36","idle_time":0,"start_time":"05:28","location":{"id":"9de52190a413483dfa572313927af794","name":"15637","address":"Las Encinas 5759, Vitacura","lat":-33.3841784,"lng":-70.58600779999999},"phone":"992394794","notes":"Alejandra Moya","original_start_time":"11:08","original_finish_time":"11:16","logged_arrival_time":1587116169837,"hasLiveTracking":true,"status":"done","expected_arrival_time":"05:40","expected_finish_time":"05:48"},{"log":[],"break":false,"arrival_time":"05:46","finish_time":"05:54","idle_time":0,"start_time":"05:46","location":{"id":"5eae651bbb3441eeb426a5ccd6d95524","name":"15639","address":"Las Condes Manuel Claro Vial 8756, Las Condes","lat":-33.4273607,"lng":-70.54157599999999},"phone":"983564747","notes":"Cintia Quintanilla","original_start_time":"11:26","original_finish_time":"11:34"},{"log":[],"break":false,"arrival_time":"05:58","finish_time":"06:06","idle_time":0,"start_time":"05:58","location":{"id":"52af6bbb59604c23ea033d263bf54fec","name":"15630","address":"Las Perdices 1387, La Reina","lat":-33.4414544,"lng":-70.533906},"phone":"998886742","notes":"Paula Jeldres Parada","notes2":"penultima casa antes de llegar a la esquina con valenzuela llanos","original_start_time":"11:38","original_finish_time":"11:46"},{"log":[],"break":false,"arrival_time":"06:14","idle_time":0,"start_time":"06:14","location":{"id":"6730387786f645aac7eb8450b2bf9e13","name":"Carlos HZGF30","address":"Camino La Fuente 1592, Las Condes, Región Metropolitana, Chile","lat":-33.4009489,"lng":-70.52161339999999},"original_start_time":"11:54"}]},"project":{"name":"test","id":"5e9971d19ba1080017cadfcc"}}'
estimated_window_minutes = 90
out_of_delivery_window_minutes = 30
route_update_event = '{"user":"ricardo@cleanqueen.cl","event":"route update","createdAt":"2020-05-05T17:28:49.521Z","routeDate":"2020-05-05","vehicle":{"start-location":{"coords":{"lat":-33.4009489,"lng":-70.52161339999999},"id":"11feea431af74d6cbe1864a26b5d53c3","address":"Camino La Fuente 1592, Las Condes, Región Metropolitana, Chile","lat":-33.4009489,"lng":-70.52161339999999},"end-location":{"coords":{"lat":-33.5105866,"lng":-70.7572607},"id":"04e8531861564859dd8ffc49cd0df34d","address":"Maipú, Maipu, Santiago Metropolitan Region, Chile","lat":-33.5105866,"lng":-70.7572607},"hidden":false,"types":[],"_id":"5ea2d50743ae5c00175332d6","name":"Patricio F","shift-start":"14:00","shift-end":"21:00","phone-number":"+56985015352","capacity":null,"breaks":[],"id":"2cd97ebde36d4711f0f9795ef321f224","_user":"5e77e7bf83b79800171ea5f6","__v":0},"solution":{"distance":144.13070000000002,"visits":[{"log":[],"break":false,"arrival_time":"14:00","idle_time":0,"start_time":"14:00","location":{"id":"11feea431af74d6cbe1864a26b5d53c3","name":"Patricio F","address":"Camino La Fuente 1592, Las Condes, Región Metropolitana, Chile","lat":-33.4009489,"lng":-70.52161339999999}},{"log":[],"break":false,"arrival_time":"14:15","finish_time":"14:23","idle_time":0,"start_time":"14:15","location":{"id":"c6be4cd33a244f51866211164b96e758","name":"16917","address":"Camino Del Monasterio 9392, Lo Barnechea","lat":-33.3357777,"lng":-70.5581014},"phone":"56993462770","notes":"Fanny Meza Schaffer"},{"log":[],"break":false,"arrival_time":"14:42","finish_time":"14:50","idle_time":0,"start_time":"14:42","location":{"id":"98ae8186878b4b08989853d5f7e135ee","name":"16988","address":"Rinconada el Salto 879. Condominio Bosques de La Pirámide, Huechuraba","lat":-33.38448899999999,"lng":-70.61128699999999},"phone":"56976090043","notes":"Valentina Nuñez Donoso"},{"log":[],"break":false,"arrival_time":"15:04","finish_time":"15:12","idle_time":0,"start_time":"15:04","location":{"id":"8964c3c443bb4e43859bc2375a93130c","name":"16892","address":"Altos del Valle 1335-2, Huechuraba","lat":-33.3433607,"lng":-70.6679333},"phone":"5699220456","notes":"Maria Carolina Nuñez Jimenez"},{"log":[],"break":false,"arrival_time":"15:36","finish_time":"15:44","idle_time":0,"start_time":"15:36","location":{"id":"2b608b8f5c9c4300b7f98e13afd0670a","name":"16980","address":"Av. Jose Rabat 9620, Colina","lat":-33.2583668,"lng":-70.6416885},"phone":"967427026","notes":"valentina correa moreno"},{"log":[],"break":false,"arrival_time":"15:58","finish_time":"16:06","idle_time":0,"start_time":"15:58","location":{"id":"f3371e85168f46d3d659af7375be20f2","name":"16890","address":"Avenida Sergio Reiss 18110, Chicureo","lat":-33.3261915,"lng":-70.62834959999999},"phone":"993596590","notes":"Javiera Bernstein Rossitto"},{"log":[],"break":false,"arrival_time":"16:19","finish_time":"16:27","idle_time":0,"start_time":"16:19","location":{"id":"f4d59a22eada4027e19838ef1d0ec5c7","name":"16991","address":"Francisco de Riveros 4343, Vitacura","lat":-33.3941009,"lng":-70.59206},"phone":"56994393454","notes":"Juanita Vial"},{"log":[],"break":false,"arrival_time":"16:32","finish_time":"16:40","idle_time":0,"start_time":"16:32","location":{"id":"4bb2082b03d14eb9930a26c99800b3f3","name":"16979","address":"Reyes Lavalle 3135, Las Condes","lat":-33.41616020000001,"lng":-70.5973733},"phone":"56984529233","notes":"Ita Rodriguez"},{"log":[],"break":false,"arrival_time":"16:43","finish_time":"16:51","idle_time":0,"start_time":"16:43","location":{"id":"83d4bc01a54049f7c1b2fb6b1fccc716","name":"16995","address":"Av. Hernando de Aguirre 959, Providencia","lat":-33.4265021,"lng":-70.5989542},"phone":"991334141","notes":"alejandra villouta"},{"log":[],"break":false,"arrival_time":"16:51","finish_time":"16:59","idle_time":0,"start_time":"16:51","location":{"id":"3052e0cd877440e1e6453be3fbc4b5db","name":"16924","address":"Av. Hernando de Aguirre 959, Providencia","lat":-33.4265021,"lng":-70.5989542},"phone":"56975784947","notes":"camila hevia stevens"},{"log":[],"break":false,"arrival_time":"17:00","finish_time":"17:08","idle_time":0,"start_time":"17:00","location":{"id":"b79be9a15a2944a2933bdafae3d1aa52","name":"17023","address":"Av. Hernando de Aguirre 1191, Providencia","lat":-33.4286447,"lng":-70.597785},"phone":"989291874","notes":"Franco Berarducci","notes2":"Anunciarse en concerjeria y bajo a recibirlo"},{"log":[],"break":false,"arrival_time":"17:09","finish_time":"17:17","idle_time":0,"start_time":"17:09","location":{"id":"22c296d7c6af49b4f0913dc78c8273a9","name":"17018","address":"Dr Roberto del Río 1110, Providencia","lat":-33.4287707,"lng":-70.59979009999999},"phone":"942802140","notes":"Veronica Bravo"},{"log":[],"break":false,"arrival_time":"17:18","finish_time":"17:26","idle_time":0,"start_time":"17:18","location":{"id":"d294659609c044e2a1c3bb680e8a35e6","name":"16972","address":"Llewellyn Jones 1485, Providencia","lat":-33.4327164,"lng":-70.60114279999999},"phone":"56972143854","notes":"Daniela Carrillo Pedrero"},{"log":[],"break":false,"arrival_time":"17:29","finish_time":"17:37","idle_time":0,"start_time":"17:29","location":{"id":"c403fc0c2174428cbac36dcd8e6fb471","name":"16931","address":"Amapolas 1500, Providencia","lat":-33.4290954,"lng":-70.5910974},"phone":"999183757","notes":"Pablo Junemann","notes2":"Dejar en portería"},{"log":[],"break":false,"arrival_time":"17:38","finish_time":"17:46","idle_time":0,"start_time":"17:38","location":{"id":"1bac130475c14a6f9d81adf59b6bc1da","name":"16973 |17000","address":"Las Dalias 2821, Providencia","lat":-33.4299726,"lng":-70.59468129999999},"phone":"988182305","notes":"Daniela Munoz"},{"log":[],"break":false,"arrival_time":"17:47","finish_time":"17:55","idle_time":0,"start_time":"17:47","location":{"id":"789e20a6686e42ee8db4d237b68d1248","name":"16999","address":"Av. Hernando de Aguirre 1420, Providencia","lat":-33.4307011,"lng":-70.596929},"phone":"95344751","notes":"Ester Saez"},{"log":[],"break":false,"arrival_time":"17:59","finish_time":"18:07","idle_time":0,"start_time":"17:59","location":{"id":"a508fabb6dce403c83a47d9f4c318038","name":"16978","address":"Diego de Almagro 4765, Ñuñoa","lat":-33.4376538,"lng":-70.5809077},"phone":"56984406729","notes":"Constanza Saez"},{"log":[],"break":false,"arrival_time":"18:08","finish_time":"18:16","idle_time":0,"start_time":"18:08","location":{"id":"3d17089222ac44abf4f0febc8104fd18","name":"16902","address":"Hamburgo 1489, Ñuñoa","lat":-33.4409771,"lng":-70.5776532},"phone":"56992464968","notes":"Pia Contreras Donoso"},{"log":[],"break":false,"arrival_time":"18:18","finish_time":"18:26","idle_time":0,"start_time":"18:18","location":{"id":"09c79aa74c69444be34c49d37f5e02ce","name":"16968","address":"Bremen 88, Ñuñoa","lat":-33.4535915,"lng":-70.5784427},"phone":"998167152","notes":"Sofia Carrasco Navarro"},{"log":[],"break":false,"arrival_time":"18:30","finish_time":"18:38","idle_time":0,"start_time":"18:30","location":{"id":"942d4bd033ee4ff9bd8c2073b27f7615","name":"16885","address":"Av. José Pedro Alessandri 264, Ñuñoa, Región Metropolitana, Chile","lat":-33.4571832,"lng":-70.5977494},"phone":"982450473","notes":"Juan Pablo Acuña","original_finish_time":"18:38","original_start_time":"18:30"},{"log":[],"break":false,"arrival_time":"18:41","finish_time":"18:49","idle_time":0,"start_time":"18:41","location":{"id":"9a016b0c631b4d2fd18dd4c354a1177c","name":"16986","address":"Av. Sucre 1993, Ñuñoa","lat":-33.4487718,"lng":-70.6114078},"phone":"985998366","notes":"Luz María Cabrera Abaroa","original_finish_time":"18:40","original_start_time":"18:32"},{"log":[],"break":false,"arrival_time":"18:53","finish_time":"19:01","idle_time":0,"start_time":"18:53","location":{"id":"88b6082050714a9a9954a3c1628de596","name":"16971","address":"Wilie Arthur 2257, Providencia","lat":-33.4430135,"lng":-70.60427539999999},"phone":"56987291447","notes":"Arlette Haeger","notes2":"Dejar en conserjería"},{"log":[],"break":false,"arrival_time":"19:05","finish_time":"19:13","idle_time":0,"start_time":"19:05","location":{"id":"f1a17b06265d45008d3d464e09c76968","name":"17009","address":"Huáscar 1400, Providencia","lat":-33.4473315,"lng":-70.61763619999999},"phone":"56968327651","notes":"ada alarcon garrido"},{"log":[],"break":false,"arrival_time":"19:18","finish_time":"19:26","idle_time":0,"start_time":"19:18","location":{"id":"2235ed8e4d9b48ad9108629485e2e2d8","name":"17001","address":"Av. Marathon 1000, Ñuñoa","lat":-33.462758,"lng":-70.6153432},"phone":"950156977","notes":"Valentina Salas","notes2":"ENVIAR SOLO ANTES DE LAS 13:00. POR FAVOR CONTACTARSE A MI CELULAR PARA COORDINA ENTREGA, NO SE PUEDE DEJAR EN PORTERÍA."},{"log":[],"break":false,"arrival_time":"19:31","finish_time":"19:39","idle_time":0,"start_time":"19:31","location":{"id":"53d0a4cb3c894cc8e390c21c5a06874e","name":"16974","address":"Vicuña Mackena 3635, San Joaquin","lat":-33.4867854,"lng":-70.62042459999999},"phone":"966063656","notes":"Angela Navarro Allende"},{"log":[],"break":false,"arrival_time":"19:44","finish_time":"19:52","idle_time":0,"start_time":"19:44","location":{"id":"50e1281e3d8e47b8867e6f3a57aff177","name":"16915","address":"Suárez Mujica 411, Ñuñoa, Región Metropolitana, Chile","lat":-33.4591786,"lng":-70.624569},"phone":"965944953","notes":"Camila Salas","original_finish_time":"19:31","original_start_time":"19:23"},{"log":[],"break":false,"arrival_time":"19:57","finish_time":"20:05","idle_time":0,"start_time":"19:57","location":{"id":"f11674ce348044c5acf3fab433597c2e","name":"16966","address":"Av. Francisco Bilbao 827, Providencia","lat":-33.4407769,"lng":-70.6219117},"phone":"990026604","notes":"maria victoria martinez muñoz","notes2":"dejar en conserjeria"},{"log":[],"break":false,"arrival_time":"20:07","finish_time":"20:15","idle_time":0,"start_time":"20:07","location":{"id":"9a46c8eaf51442b9b7e3f7a14bfbfe77","name":"17020","address":"Rafael Cañas 115, Providencia","lat":-33.4323649,"lng":-70.62311559999999},"phone":"56933853582","notes":"Sandra Galaz"},{"log":[],"break":false,"arrival_time":"20:18","finish_time":"20:26","idle_time":0,"start_time":"20:18","location":{"id":"2ffecde2f9fa4d11ca05120fea400f3b","name":"16948","address":"José Miguel de La Barra 450, Bellas Artes","lat":-33.4353465,"lng":-70.643551},"phone":"968305179","notes":"Jaime Nahum König"},{"log":[],"break":false,"arrival_time":"20:29","finish_time":"20:37","idle_time":0,"start_time":"20:29","location":{"id":"45c86e911b714a3cea596e1aeeba61de","name":"17025","address":"Santa Victoria 538, Santiago Centro","lat":-33.4489134,"lng":-70.6425223},"phone":"951280057","notes":"Jose Carlos Espinoza","notes2":"Dejar en conserjeria"},{"log":[],"break":false,"arrival_time":"20:40","finish_time":"20:48","idle_time":0,"start_time":"20:40","location":{"id":"152f4fadce1042eca6930de05294fa67","name":"16982","address":"Nataniel Cox 331, Santiago, Región Metropolitana, Chile","lat":-33.4502904,"lng":-70.6535356},"phone":"962119591","notes":"Leonor Levill","notes2":"Dejar en conserjería (Nataniel Cox 331 depto. 1305) Leonor Levill"},{"log":[],"break":false,"arrival_time":"20:52","finish_time":"21:00","idle_time":0,"start_time":"20:52","location":{"id":"8c0530492e3a4546c5da65d6e61c549d","name":"16903","address":"MATURANA 164, santiago","lat":-33.4428823,"lng":-70.6665958},"phone":"985039865","notes":"viviana riquelme","notes2":"TIMBRE MALO, LLAMAR AL 226955370 AL LLEGAR"},{"log":[],"break":false,"arrival_time":"21:15","idle_time":0,"start_time":"21:15","location":{"id":"04e8531861564859dd8ffc49cd0df34d","name":"Patricio F","address":"Maipú, Maipu, Santiago Metropolitan Region, Chile","lat":-33.5105866,"lng":-70.7572607}}]},"project":{"name":"Salida Martes 5 de Mayo","id":"5eb1743f72396b0017407c2f"}}'
event = json.loads(route_update_event)
event_types = dict()
def process_event(event):
    """
    if event['event'] not in event_types:
        event_types.append(event['event'])
        """
    if event['event'] == "route update":
        #identify the visits that are no longer in the route and take actions if the client was notified with the arrival time
        #register or update the visits
        #vehicle
        vehicle_id = event['vehicle']['id']
        vehicle_name = event['vehicle']['name']
        vehicle_shift_start = event['vehicle']['shift-start']
        vehicle_shift_end = event['vehicle']['shift-end']
        vehicle_phone_number = event['vehicle']['phone-number']
        
        query_cursor.execute("Select * from vehicles where id='{0}' limit 1;".format(vehicle_id))
        vehicle_already_registered = query_cursor.fetchone()
        if(vehicle_already_registered):
            query  = "Update vehicles set name='{}', shift_start='{}', shift_end='{}' where id='{}';".format(vehicle_name,vehicle_shift_start, vehicle_shift_end,vehicle_id)
            query_cursor.execute(query)
        else:
            query_cursor.execute("Insert into vehicles (id, name, shift_start, shift_end) values ('{}','{}','{}','{}')".format(vehicle_id, vehicle_name,vehicle_shift_start, vehicle_shift_end))
        
        conn.commit()
        print("vehicle ",query_cursor.statusmessage)
        project_id = event['project']['id']
        project_name = event['project']['name']

        query_cursor.execute("select * from projects where id='{}' limit 1;".format(project_id))
        project_already_registeded = query_cursor.fetchone()
        if project_already_registeded:
            query = "Update projects set name='{}' where id ='{}'".format(project_name, project_id)
            query_cursor.execute(query)
        else:
            query = "Insert into projects(id, name) values ('{}','{}')".format(project_id, project_name)
            query_cursor.execute(query)

        conn.commit()
        print("project ", query_cursor.statusmessage)

        touple = (event['event'], vehicle_id, project_id)

        event_types[touple] = event_types.get(touple,[]) + [event]
        ##process the visits
        #check the visits that are not longer in the vehicle
    
        served_locations_ids = []
        for visit in event['solution']['visits']:
            location_id = visit['location']['id']
            served_locations_ids.append(location_id)
            location_name = visit['location']['name']
            location_address = visit['location']['address']
            location_lat = visit['location']['lat']
            location_long = visit['location']['lng']        

            status=visit['status'] if 'status' in visit else None
            arrival_time = visit['arrival_time'] if 'arrival_time' in visit else None
            finish_time = visit['finish_time'] if 'finish_time' in visit else None
            expected_arrival_time = visit['expected_arrival_time'] if 'expected_arrival_time' in visit else None
            expected_finish_time = visit['expected_finish_time'] if 'expected_finish_time' in visit else None
            if(visit == event['solution']['visits'][-1] or visit == event['solution']['visits'][0]):
                #the first and last visit are of the driver and only has arrival time (not finish)
                continue

            if 'status' in visit:
               #the visit has been arrived or done.
               pass 
            
            query_cursor.execute("Select * from visits where location_id='{}'".format(location_id))
        
            registered_visit = query_cursor.fetchone()
            if(registered_visit):
                query = "Update visits set vehicle_id='{}', location_name='{}', location_address='{}', location_long={}, location_lat={}, arrival_time='{}', finish_time='{}', estimated_arrival_time='{}', estimated_finish_time ='{}'"," where location_id='{}'".format(vehicle_id, location_name, location_address, location_long, location_lat, arrival_time, finish_time, expected_arrival_time, expected_finish_time, status, location_id)
            else:
                query = "Insert into visits (vehicle_id, project_id, location_id, location_name, location_address, location_long, location_lat, arrival_time, finish_time, estimated_arrival_time, estimated_finish_time,status) values('{}','{}','{}','{}','{}', {}, {},'{}','{}','{}','{}', '{}')".format(vehicle_id, project_id, location_id, location_name, location_address, location_long, location_lat, arrival_time, finish_time, expected_arrival_time, expected_finish_time,status)
                query_cursor.execute(query)

            conn.commit()

            print ("visit ", query_cursor.statusmessage)
        query = "Select * from visits where vehicle_id='{}' and project_id='{}' and location_id not in {}".format(vehicle_id, project_id, tuple(served_locations_ids))
        query_cursor.execute(query)
        no_longer_visits = query_cursor.fetchall()
        #take desicions for the no longer visits
        """
            this_visit_has_been_registered = False

            if not this_visit_has_been_registered:
                #register the visit
                pass
                #send mail to client
                pass
            else:
                #update the fields in the visit

                estimated_arrival_time_has_changed_too_much = True
                estimated_arrival_time_has_changed_too_much = False
                if (estimated_arrival_time_has_changed_too_much):
                    #send mail to client
                    pass
               """ 

    elif event['event'] == "visit status update":
        project_id = event['project']['id']
        vehicle = event['vehicle']
        updated_visit = event['visit']
        visit_status = updated_visit['status']
        if visit_status == 'done':
            #TODO:notify the user that the visit has been completed
            pass
        elif visit_status == 'skipped':
            #TODO: notify the user that the visit has been completed
            pass
        elif visit_status == 'arrived':
            #TODO: notify the user that the visit has arrived            
            pass
        #TODO: update the current visit into db
        #
        route_status = event['vehicle']['routeStatus']        
        updated_visit_index_in_route = route_status.index(updated_visit)
        following_visits= route_status[updated_visit_index_in_route+1:]
        last_visit_is_vehicle_end_location = following_visits[-1]['location']['id'] == vehicle['end-location']['id']
        if last_visit_is_vehicle_end_location:
            following_visits = following_visits[:-1]

        if updated_visit_index_in_route == 0:
            for visit in following_visits:
                #update the fields

                is_break = visit['break']
                arrival_time = visit['arrival_time'] 
                finish_time = visit['finish_time']
                expected_arrival_time = visit['expected_arrival_time'] if 'expected_arrival_time' in visit else None
                expected_finish_time = visit['expected_finish_time'] if 'expected_finish_time' in visit else None
                
                phone_number = visit['phone']
                status = visit['status'] if 'status' in visit else None
                notes = visit['notes'] if 'notes' in visit else None
                notes2 = visit['notes2'] if 'notes2' in visit else None

                location_id = visit['location']['id']
                location_name = visit['location']['name'] if 'name' in visit['location'] else None
                location_address = visit['location']['address']
                location_lat = visit['location']['lat']
                location_long = visit['location']['lng']

                #TODO: update the visit in the database
                query_registered_visit = "Select id"
                registered_visit = ""
                #TODO: notify estimated arrival time with the phone number

        # notify the following visit's user 
        following_visit = route_status[updated_visit_index_in_route + 1]
        following_client_wants_to_know_when_near = False
        if following_client_wants_to_know_when_near:
            ##notify following visit
            pass

conn = psycopg2.connect(
    host="localhost",
    database="clean_queen_routes",
    user="postgres",
    password="postgres")

query_cursor = conn.cursor()
events_file = open('event_jsons.txt') 

for i,line in enumerate(events_file):
    
    if (line[-2] == "|"):
        line = line[:-2]
    if (line[0] == "|"):
        line = line[1:]

    line.replace('\n',' ')
    #print ("line ",i)
    try:
        event = json.loads(line)
        process_event(event)
    except:
        pass
    #print("ok")
pass