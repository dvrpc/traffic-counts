/*
  All database changes made as part of this new import program.
*/

-- Add column for unclassified vehicles, which we will now start tracking.
alter table tc_clacount add unclassified number;

-- Add count direction.
-- (already in tc_spesum and tc_specount tables as ctdir and in tc_header as cntdir)
alter table tc_volcount add cntdir varchar2(10);
alter table tc_15minvolcount add cntdir varchar2(10);

-- Constrain direction values for tables it was already on and ones it is being added to.
-- NOTE multiple fields: cntdir, ctdir, trafdir
-- tables these fields already existed on
alter table tc_header add constraint cntdir_tc_header check (cntdir in ('north', 'east', 'west', 'south', 'both') );
alter table tc_header add constraint trafdir_tc_header check (trafdir in ('north', 'east', 'west', 'south', 'both') );
-- note these fields are "ctdir", not "cntdir"
alter table tc_clacount add constraint ctdir_tc_clacount check (ctdir in ('north', 'east', 'west', 'south') );
alter table tc_specount add constraint ctdir_tc_specount check (ctdir in ('north', 'east', 'west', 'south') );
-- tables field was added to
alter table tc_volcount add constraint cntdir_tc_volcount check (cntdir in ('north', 'east', 'west', 'south') );
alter table tc_15minvolcount add constraint cntdir_tc_15minvolcount check (cntdir in ('north', 'east', 'west', 'south') );


-- Add count lane.
-- (already in tc_spesum and tc_specount tables)
alter table tc_volcount add countlane number(2,0);
alter table tc_15minvolcount add countlane number(2,0);

-- Constrain values for countlane.
-- the tables the field had already been on - tc_spesum and tc_specount
alter table tc_spesum add constraint countlane_valid_spesum CHECK (countlane >= 1 AND countlane <= 3);
alter table tc_specount add constraint countlane_valid_specount CHECK (countlane >= 1 AND countlane <= 3);
-- the new columns we just added
alter table tc_volcount add constraint countlane_valid_volcount CHECK (countlane >= 1 AND countlane <= 3);
alter table tc_15minvolcount add constraint countlane_valid_15minvolcount CHECK (countlane >= 1 AND countlane <= 3);

/*
  BEGIN PRIMARY KEY
  Change primary key on tc_volcount and tc_15minvolcount.
  Because we're now including records by cntdir, the primary key for these tables
  needs to be altered. It was previously a unique constraint on (recordnum, countdate) for tc_volcount and (recordnum, countdate, counttime) for tc_15minvolcount.
  cntdir now needs to be included, since there can be two directions that have the other values.
*/
-- Add new column for primary key.
alter table tc_volcount add id number;
alter table tc_15minvolcount add id number;

-- Add sequence for it.
create sequence tc_volcount_pk_seq start with 1 increment by 1 nomaxvalue;
create sequence tc_15minvolcount_pk_seq start with 1 increment by 1 nomaxvalue;

-- Create new trigger.
create or replace trigger tc_volcount_pk_trigger
    before insert on tc_volcount
    for each row
begin
    select tc_volcount_pk_seq.nextval 
    into :new.id 
    from dual;
end;
/

create or replace trigger tc_15minvolcount_pk_trigger
    before insert on tc_15minvolcount
    for each row
begin
    select tc_15minvolcount_pk_seq.nextval 
    into :new.id 
    from dual;
end;
/

-- Add a value for that column.
UPDATE tc_volcount SET id = tc_volcount_pk_seq.nextval;
UPDATE tc_15minvolcount SET id = tc_15minvolcount_pk_seq.nextval;

-- Drop old primary key and its index.
alter table tc_volcount drop constraint tc_volcount_pk drop index;
alter table tc_15minvolcount drop constraint tc_15minvolcount_pk drop index; 

-- Add a primary key constraint and index.
alter table tc_volcount add constraint tc_volcount_pk primary key (id);
alter table tc_15minvolcount add constraint tc_15minvolcount_pk primary key (id);

-- Add new unique constraint to reflect how data will be stored.
alter table tc_volcount 
    add constraint unique_record_date_dir_lane_volcount unique (recordnum, countdate, cntdir, countlane);
alter table tc_15minvolcount 
    add constraint unique_record_datetime_dir_lane_15minvolcount unique (recordnum, countdate, counttime, cntdir, countlane);
/*
  END PRIMARY KEY
*/

-- Create table annual average daily volume, by directionality.
create table aadv (
    recordnum number not null,
    aadv number not null,
    direction varchar2(5),
    date_calculated date not null
);

-- Add aadv column to tc_header, for redundant storage of latest aadv.
alter table tc_header add aadv number;

/*
 Begin enabling custom factors
 Factors for 5 NJ municipalities have been kept out of the database since 2017, include them.
 (If more are needed, we will add additional columns to tc_factor and update the records
 in the tc_mcd table to note which MCDs they apply to.)
*/
alter table tc_factor add nj_region4_factor number;
alter table tc_factor add nj_region4_axle number;

alter table tc_mcd add custom_factor varchar2(100);
alter table tc_mcd add custom_axle_factor varchar2(100);

-- "dvrpc" column here is MCD
UPDATE tc_mcd
    SET custom_factor = 'nj_region4_factor', custom_axle_factor = 'nj_region4_axle' 
    where dvrpc = 3400503370; -- Bass River Twp, Burlington
UPDATE tc_mcd
    set custom_factor = 'nj_region4_factor', custom_axle_factor = 'nj_region4_axle' 
    where dvrpc = '3400577150'; -- Washington Twp, Burlington
update tc_mcd
    set custom_factor = 'nj_region4_factor',  custom_axle_factor = 'nj_region4_axle' 
    where dvrpc = '3400582420'; -- Woodland Twp, Burlington
update tc_mcd
    set custom_factor = 'nj_region4_factor', custom_axle_factor = 'nj_region4_axle' 
    where dvrpc = '3401524840'; -- Franklin Twp, Gloucester
update tc_mcd
    set custom_factor = 'nj_region4_factor', custom_axle_factor = 'nj_region4_axle' 
    where dvrpc = '3401551390'; -- Newfield Boro, Gloucester

-- End enabling custom factors

-- Create table to store results from running imports.
create table import_log (
    recordnum number not null,
    datetime date default current_date,
    message varchar2(1000) not null,
    log_level varchar2(10) not null
);


-- Create table to store days (official U.S. holidays, at least) to exclude from AADV calculations.
create table aadv_excluded_days (
    excluded_day date unique not null,
    reason varchar2(500) not null,
    client varchar(100)
);

-- 2023 and 2024 excluded days
-- This is temporary/not how they will be added in future; a user interface will be created.
-- U.S. holidays from <https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/>
-- 2023
insert into aadv_excluded_days values (to_date('2023-01-02', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-01-16', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-02-20', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-05-29', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-06-19', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-07-04', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-09-04', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-10-09', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-11-10', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-11-23', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2023-12-25', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
--2024
insert into aadv_excluded_days values (to_date('2024-01-01', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-01-15', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-02-19', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-05-27', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-06-19', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-07-04', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-09-02', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-10-14', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-11-11', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-11-28', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
insert into aadv_excluded_days values (to_date('2024-12-25', 'YYYY-MM-DD'), 'U.S. holiday', NULL);
--2024 additional PennDot excluded days
insert into aadv_excluded_days values (to_date('2024-03-28', 'YYYY-MM-DD'), 'Easter', 'PennDot');
insert into aadv_excluded_days values (to_date('2024-04-01', 'YYYY-MM-DD'), 'Easter', 'PennDot');
insert into aadv_excluded_days values (to_date('2024-05-23', 'YYYY-MM-DD'), 'Memorial Day', 'PennDot');
insert into aadv_excluded_days values (to_date('2024-05-28', 'YYYY-MM-DD'), 'Memorial Day', 'PennDot');
insert into aadv_excluded_days values (to_date('2024-07-03', 'YYYY-MM-DD'), 'Independence Day', 'PennDot');
