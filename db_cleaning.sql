-- Normalize/fix indir values.
update tc_header set indir = 'north' where indir in ('N', 'n', 'North');
update tc_header set indir = 'south' where indir in ('S', 's', 'South');
update tc_header set indir = 'east' where indir in ('E', 'e', 'East');
update tc_header set indir = 'west' where indir in ('W', 'w', 'West');
-- Recordnum 68250 has a value of '0999' here, but it's a 15 min Volume type, so should be null
update tc_header set indir = Null where indir = '0999';


-- Normalize/fix outdir values.
update tc_header set outdir = 'north' where outdir in ('N', 'n', 'North');
update tc_header set outdir = 'south' where outdir in ('S', 's', 'South');
update tc_header set outdir = 'east' where outdir in ('E', 'e', 'East');
update tc_header set outdir = 'west' where outdir in ('W', 'w', 'West');


-- Normalize/fix sidewalk values.
update tc_header set sidewalk = 'north' where sidewalk in ('N', 'n', 'North');
update tc_header set sidewalk = 'south' where sidewalk in ('S', 's', 'South');
update tc_header set sidewalk = 'east' where sidewalk in ('E', 'e', 'East');
update tc_header set sidewalk = 'west' where sidewalk in ('W', 'w', 'West');
-- Some (96313, 96314, 96315, 96316) have a value of '25'. These are null on other records with
-- matching fromlmt/tolmt, same make these null as well.
update tc_header set sidewalk = Null where sidewalk = '25';


-- Normalize/fix cntdir values in tc_specount table.
update tc_specount set cntdir = 'north' where cntdir in ('N', 'n', 'North');
update tc_specount set cntdir = 'south' where cntdir in ('S', 's', 'South');
update tc_specount set cntdir = 'east' where cntdir in ('E', 'e', 'East');
update tc_specount set cntdir = 'west' where cntdir in ('W', 'w', 'West');


-- Normalize/fix cntdir values in tc_clacount table.
update tc_clacount set cntdir = 'north' where cntdir in ('N', 'n', 'North');
update tc_clacount set cntdir = 'south' where cntdir in ('S', 's', 'South');
update tc_clacount set cntdir = 'east' where cntdir in ('E', 'e', 'East');
update tc_clacount set cntdir = 'west' where cntdir in ('W', 'w', 'West');

-- Clean values (but no constraint was added nor type change made to prevent
-- future bad values from being entered).
update tc_header set source = 'Y' where source in ('-1', 'Yes', 'yes');
update tc_header set source = 'N' where source in ('0', 'No', 'no');
update tc_header set divided = 'Y' where divided in ('-1', 'Yes', 'yes');
update tc_header set divided = 'N' where divided in ('0', 'No', 'no');
update tc_header set hpms = 'Y' where hpms in ('-1', 'Yes', 'yes');
update tc_header set hpms = 'N' where hpms in ('0', 'No', 'no');


-- Previously done on both prod and test database:
--   - trafdir normalized/fixed
--   - cntdir normalized/fixed
--   - speedlimit: fixed - nonsensical values set to Null
--   - countlane in tc_clacount and tc_specount: fixed
--   - `update tc_counttype set factor2 = 1.0622 where counttype = 'Pedestrian'`: this
--     had previously been hardcoded in a calculation somewhere, now in both test and prod dbs.
--   - fix bad value for divided in tc_header recordnum 60142: all the other values for divided
--     for counts with same route, tolmt, and fromlmt where null, so I made this one null as well
