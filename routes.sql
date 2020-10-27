CREATE TABLE IF NOT EXISTS projects (
   id text primary key,
   name text,
   project_date date
   );
   
CREATE TABLE if not exists vehicles (
	id text primary key,
	name text,
	shift_start time,
	shift_end time,
    phone_number text,

    start_location_id text,
    start_location_address text,
    start_location_lat int,
    start_location_long int,

    end_location_id text,
    end_location_address text,
    end_location_lat int,
    end_location_long int

);

create table  visits(
    id bigserial primary key,
    project_id text references projects(id),
    vehicle_id text references vehicles(id),
    
	is_break boolean,
    estimated_arrival_time time,
    estimated_finish_time time,
    arrival_time time,
    finish_time time,
    status text,
    phone_number varchar(11),
    notes text,
    notes2 text,
    location_id text,
    location_name text,
    location_address text,
    location_lat integer,
    location_long integer,

    is_vehicle_start_location boolean,
    is_vehicle_end_location boolean,
    
    notified_out_of_delivery boolean,
    notified_planned_delivery boolean,
    notified_cancelled boolean,
    notified_completed boolean
);

create table if not exists notifications(
    project_id text references projects(id),
    location_id text,
    created_at timestamp,
    notification_type text
);

create table if not exists custom_notes(
    int bigserial,
    visit_id bigserial references visits(id),
    field_key text,
    field_value text
);


create table vehicle_breaks(
	id text primary key,
	vehicle_id text references vehicles(id)
);

create table vehicle_capacities (
	vehicle_id text references vehicles(id),
	capacity_type text,
	capacity_number integer
)


