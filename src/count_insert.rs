//! Insert/replace count data in the database.
//!
//! See the [CountInsert trait implementors][CountInsert#implementors] for kinds of counts
//! and associated tables.

use oracle::{sql_type::Timestamp, Connection, Statement};

use crate::{
    denormalize::{NonNormalAvgSpeedCount, NonNormalVolCount},
    *,
};

/// A trait for inserting/replacing count data.
pub trait CountInsert {
    /// The name of the table in the database that this count type corresponds to.
    const COUNT_TABLE: &'static str; // associated constant
    /// Field in COUNT_TABLE with recordnum.
    const COUNT_RECORDNUM_FIELD: &'static str;

    /// Delete all records in the table with a particular recordnum.
    fn delete(conn: &Connection, recordnum: u32) -> Result<(), oracle::Error> {
        let sql = &format!(
            "delete from {} where {} = :1",
            &Self::COUNT_TABLE,
            &Self::COUNT_RECORDNUM_FIELD
        );
        conn.execute(sql, &[&recordnum])?;
        conn.commit()
    }

    /// Create prepared statement to use for insert.
    fn prepare_insert(conn: &Connection) -> Result<Statement<'_>, oracle::Error>;

    /// Insert a record into the table using prepared statement.
    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error>;
}

impl CountInsert for TimeBinnedVehicleClassCount {
    const COUNT_TABLE: &'static str = "tc_clacount";
    const COUNT_RECORDNUM_FIELD: &'static str = "recordnum";

    fn prepare_insert(conn: &Connection) -> Result<Statement<'_>, oracle::Error> {
        let sql = &format!(
            "insert into {} (recordnum, countdate, counttime, countlane, total, ctdir, \
            bikes, cars_and_tlrs, ax2_long, buses, ax2_6_tire, ax3_single, ax4_single, \
            lt_5_ax_double, ax5_double, gt_5_ax_double, lt_6_ax_multi, ax6_multi, gt_6_ax_multi, \
            unclassified)
            VALUES \
            (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, 
            :19, :20)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }
    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        let oracle_date = Timestamp::new(
            self.datetime.year(),
            self.datetime.month() as u32,
            self.datetime.day() as u32,
            0,
            0,
            0,
            0,
        );
        // COUNTTIME is ok to be full datetime
        let oracle_dt = Timestamp::new(
            self.datetime.year(),
            self.datetime.month() as u32,
            self.datetime.day() as u32,
            self.datetime.hour() as u32,
            self.datetime.minute() as u32,
            self.datetime.second() as u32,
            0,
        );

        stmt.execute(&[
            &self.record_num,
            &oracle_date,
            &oracle_dt,
            &self.lane,
            &self.total,
            &format!("{}", self.direction),
            &self.c1,
            &self.c2,
            &self.c3,
            &self.c4,
            &self.c5,
            &self.c6,
            &self.c7,
            &self.c8,
            &self.c9,
            &self.c10,
            &self.c11,
            &self.c12,
            &self.c13,
            &self.c15,
        ])
    }
}
impl CountInsert for TimeBinnedSpeedRangeCount {
    const COUNT_TABLE: &'static str = "tc_specount";
    const COUNT_RECORDNUM_FIELD: &'static str = "recordnum";

    fn prepare_insert(conn: &Connection) -> Result<Statement<'_>, oracle::Error> {
        let sql = &format!(
            "insert into {} (
            recordnum, countdate, counttime, countlane, total, ctdir, \
            s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14)
            VALUES \
            (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, 
            :19, :20)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }
    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        let oracle_date = Timestamp::new(
            self.datetime.year(),
            self.datetime.month() as u32,
            self.datetime.day() as u32,
            0,
            0,
            0,
            0,
        );
        // COUNTTIME is ok to be full datetime
        let oracle_dt = Timestamp::new(
            self.datetime.year(),
            self.datetime.month() as u32,
            self.datetime.day() as u32,
            self.datetime.hour() as u32,
            self.datetime.minute() as u32,
            self.datetime.second() as u32,
            0,
        );

        stmt.execute(&[
            &self.record_num,
            &oracle_date,
            &oracle_dt,
            &self.lane,
            &self.total,
            &format!("{}", self.direction),
            &self.s1,
            &self.s2,
            &self.s3,
            &self.s4,
            &self.s5,
            &self.s6,
            &self.s7,
            &self.s8,
            &self.s9,
            &self.s10,
            &self.s11,
            &self.s12,
            &self.s13,
            &self.s14,
        ])
    }
}

impl CountInsert for NonNormalAvgSpeedCount {
    const COUNT_TABLE: &'static str = "tc_spesum";
    const COUNT_RECORDNUM_FIELD: &'static str = "recordnum";

    fn prepare_insert(conn: &Connection) -> Result<Statement<'_>, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (recordnum, countdate, ctdir, countlane, \
            am12, am1, am2, am3, am4, am5, am6, am7, am8, am9, am10, am11, pm12, \
            pm1, pm2, pm3, pm4, pm5, pm6, pm7, pm8, pm9, pm10, pm11)
            VALUES \
            (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, 
            :19, :20, :21, :22, :23, :24, :25, :26, :27, :28)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        let oracle_date = Timestamp::new(
            self.date.year(),
            self.date.month() as u32,
            self.date.day() as u32,
            0,
            0,
            0,
            0,
        );

        stmt.execute(&[
            &self.record_num,
            &oracle_date,
            &format!("{}", self.direction),
            &self.lane,
            &self.am12,
            &self.am1,
            &self.am2,
            &self.am3,
            &self.am4,
            &self.am5,
            &self.am6,
            &self.am7,
            &self.am8,
            &self.am9,
            &self.am10,
            &self.am11,
            &self.pm12,
            &self.pm1,
            &self.pm2,
            &self.pm3,
            &self.pm4,
            &self.pm5,
            &self.pm6,
            &self.pm7,
            &self.pm8,
            &self.pm9,
            &self.pm10,
            &self.pm11,
        ])
    }
}

impl CountInsert for NonNormalVolCount {
    const COUNT_TABLE: &'static str = "tc_volcount";
    const COUNT_RECORDNUM_FIELD: &'static str = "recordnum";

    fn prepare_insert(conn: &Connection) -> Result<Statement<'_>, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (recordnum, countdate, setflag, totalcount, weather, cntdir, countlane, \
            am12, am1, am2, am3, am4, am5, am6, am7, am8, am9, am10, am11, pm12, \
            pm1, pm2, pm3, pm4, pm5, pm6, pm7, pm8, pm9, pm10, pm11)
            VALUES \
            (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, 
            :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        let oracle_date = Timestamp::new(
            self.date.year(),
            self.date.month() as u32,
            self.date.day() as u32,
            0,
            0,
            0,
            0,
        );

        stmt.execute(&[
            &self.record_num,
            &oracle_date,
            &"", // setflag
            &self.totalcount,
            &"", // weather
            &format!("{}", self.direction),
            &self.lane,
            &self.am12,
            &self.am1,
            &self.am2,
            &self.am3,
            &self.am4,
            &self.am5,
            &self.am6,
            &self.am7,
            &self.am8,
            &self.am9,
            &self.am10,
            &self.am11,
            &self.pm12,
            &self.pm1,
            &self.pm2,
            &self.pm3,
            &self.pm4,
            &self.pm5,
            &self.pm6,
            &self.pm7,
            &self.pm8,
            &self.pm9,
            &self.pm10,
            &self.pm11,
        ])
    }
}

impl CountInsert for FifteenMinuteVehicle {
    const COUNT_TABLE: &'static str = "tc_15minvolcount";
    const COUNT_RECORDNUM_FIELD: &'static str = "recordnum";

    fn prepare_insert(conn: &Connection) -> Result<Statement<'_>, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (recordnum, countdate, counttime, volcount, cntdir, countlane) \
            VALUES (:1, :2, :3, :4, :5, :6)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        let oracle_date = Timestamp::new(
            self.date.year(),
            self.date.month() as u32,
            self.date.day() as u32,
            0,
            0,
            0,
            0,
        );
        // COUNTTIME is ok to be full datetime
        let oracle_dt = Timestamp::new(
            self.date.year(),
            self.date.month() as u32,
            self.date.day() as u32,
            self.time.hour() as u32,
            self.time.minute() as u32,
            self.time.second() as u32,
            0,
        );

        stmt.execute(&[
            &self.record_num,
            &oracle_date,
            &oracle_dt,
            &self.count,
            &format!("{}", self.direction),
            &self.lane,
        ])
    }
}

impl CountInsert for FifteenMinuteBicycle {
    const COUNT_TABLE: &'static str = "tc_bikecount";
    const COUNT_RECORDNUM_FIELD: &'static str = "dvrpcnum";

    fn prepare_insert(conn: &Connection) -> Result<Statement<'_>, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (dvrpcnum, countdate, counttime, total, incount, outcount) \
            VALUES (:1, :2, :3, :4, :5, :6)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        let oracle_date = Timestamp::new(
            self.date.year(),
            self.date.month() as u32,
            self.date.day() as u32,
            0,
            0,
            0,
            0,
        );
        // COUNTTIME is ok to be full datetime
        let oracle_dt = Timestamp::new(
            self.date.year(),
            self.date.month() as u32,
            self.date.day() as u32,
            self.time.hour() as u32,
            self.time.minute() as u32,
            self.time.second() as u32,
            0,
        );

        stmt.execute(&[
            &self.record_num,
            &oracle_date,
            &oracle_dt,
            &self.total,
            &self.indir,
            &self.outdir,
        ])
    }
}

impl CountInsert for FifteenMinutePedestrian {
    const COUNT_TABLE: &'static str = "tc_pedcount";
    const COUNT_RECORDNUM_FIELD: &'static str = "dvrpcnum";

    fn prepare_insert(conn: &Connection) -> Result<Statement<'_>, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (dvrpcnum, countdate, counttime, total, \"IN\", \"OUT\") \
            VALUES (:1, :2, :3, :4, :5, :6)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        let oracle_date = Timestamp::new(
            self.date.year(),
            self.date.month() as u32,
            self.date.day() as u32,
            0,
            0,
            0,
            0,
        );
        // COUNTTIME is ok to be full datetime
        let oracle_dt = Timestamp::new(
            self.date.year(),
            self.date.month() as u32,
            self.date.day() as u32,
            self.time.hour() as u32,
            self.time.minute() as u32,
            self.time.second() as u32,
            0,
        );

        stmt.execute(&[
            &self.record_num,
            &oracle_date,
            &oracle_dt,
            &self.total,
            &self.indir,
            &self.outdir,
        ])
    }
}
