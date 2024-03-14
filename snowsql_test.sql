create or replace database sf_tuts;


-- check current database
select current_database(), current_schema();

create or replace table emp_basic (
       first_name string,
       last_name string ,
       email string ,
       streetaddress string ,
       city string ,
       start_date date
);

-- create a virtual warehouse
create or replace warehouse sf_tuts_wh with
  warehouse_size='X-SMALL'
  auto_suspend = 180
  auto_resume = true
  initially_suspended=true;


-- uploading data
put file:///tmp/employees0*.csv @<database-name>.<schema-name>.%<table-name>;

put file:///tmp/employees0*.csv @sf_tuts.public.%emp_basic;


-- list staged files

list @<database-name>.<schema-name>.%<table-name>;

-- finally copy into table

copy into emp_basic
  from @%emp_basic
  file_format = (type = csv field_optionally_enclosed_by='"')
  pattern = '.*employees0[1-5].csv.gz'
  on_error = 'skip_file';


-- queries

select * from emp_basic where first_name = 'Ron';

insert into emp_basic values
  ('Clementine','Adamou','cadamou@sf_tuts.com','10510 Sachs Road','Klenak','2017-9-22') ,
  ('Marlowe','De Anesy','madamouc@sf_tuts.co.uk','36768 Northfield Plaza','Fangshan','2017-1-26');


drop database if exists sf_tuts;

drop warehouse if exists sf_tuts_wh;
