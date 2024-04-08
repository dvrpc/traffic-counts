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
    add constraint unique_record_date_dir_volcount unique (recordnum, countdate, cntdir);
alter table tc_15minvolcount 
    add constraint unique_record_datetime_dir_15minvolcount unique (recordnum, countdate, counttime, cntdir);
/*
  END PRIMARY KEY
*/
