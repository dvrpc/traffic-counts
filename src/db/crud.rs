//! Basic CRUD db operations on count data tables.
//!
//! See the [Crud trait implementors][Crud#implementors] for kinds of counts and associated tables.

use oracle::{Connection, Statement};

use chrono::NaiveDateTime;

use crate::{
    CountError, FifteenMinuteBicycle, FifteenMinutePedestrian, FifteenMinuteVehicle,
    HourlyAvgSpeed, HourlyVehicle, TimeBinnedSpeedRangeCount, TimeBinnedVehicleClassCount,
};

/// A trait for handling basic CRUD db operations on count data tables.
pub trait Crud {
    /// The name of the table in the database that this count type corresponds to.
    const COUNT_TABLE: &'static str; // associated constant

    /// Select all records from the table.
    fn select(conn: &Connection, recordnum: u32) -> Result<Vec<Self>, CountError>
    where
        Self: std::marker::Sized + oracle::RowValue,
    {
        let sql = &format!("select * FROM {} where recordnum = :1", &Self::COUNT_TABLE,);
        let results = conn.query_as::<Self>(sql, &[&recordnum])?;

        let mut data = vec![];
        for result in results {
            let result = result?;
            data.push(result);
        }
        Ok(data)
    }

    /// Delete all records in the table with a particular recordnum.
    fn delete(conn: &Connection, recordnum: u32) -> Result<(), oracle::Error> {
        let sql = &format!("delete from {} where recordnum = :1", &Self::COUNT_TABLE,);
        conn.execute(sql, &[&recordnum])?;
        conn.commit()
    }

    /// Create prepared statement to use for insert.
    fn prepare_insert(conn: &Connection) -> Result<Statement, oracle::Error>;

    /// Insert a record into the table using prepared statement.
    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error>;
}

impl Crud for TimeBinnedVehicleClassCount {
    const COUNT_TABLE: &'static str = "tc_clacount_new";

    fn prepare_insert(conn: &Connection) -> Result<Statement, oracle::Error> {
        let sql = &format!(
            "insert into {} (recordnum, countdatetime, countlane, total, cntdir, \
            bikes, cars_and_tlrs, ax2_long, buses, ax2_6_tire, ax3_single, ax4_single, \
            lt_5_ax_double, ax5_double, gt_5_ax_double, lt_6_ax_multi, ax6_multi, gt_6_ax_multi, \
            unclassified)
            VALUES \
            (:1, :2, :3, :4, :5, :6, :7, :8, :9, :11, :12, :13, :14, :15, :16, :17, :18, 
            :19, :20)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }
    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        stmt.execute(&[
            &self.recordnum,
            &NaiveDateTime::new(self.date, self.time.time()),
            &self.lane,
            &self.total,
            &self.direction,
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
impl Crud for TimeBinnedSpeedRangeCount {
    const COUNT_TABLE: &'static str = "tc_specount_new";

    fn prepare_insert(conn: &Connection) -> Result<Statement, oracle::Error> {
        let sql = &format!(
            "insert into {} (
            recordnum, countdatetime, countlane, total, cntdir, \
            s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14)
            VALUES \
            (:1, :2, :3, :4, :5, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, 
            :19, :20)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        stmt.execute(&[
            &self.recordnum,
            &NaiveDateTime::new(self.date, self.time.time()),
            &self.lane,
            &self.total,
            &self.direction,
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

impl Crud for HourlyAvgSpeed {
    const COUNT_TABLE: &'static str = "tc_spesum_new";
    fn prepare_insert(conn: &Connection) -> Result<Statement, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (recordnum, countdatetime, avgspeed, countlane, cntdir) \
            VALUES (:1, :2, :3, :4, :5)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        stmt.execute(&[
            &self.recordnum,
            &self.datetime,
            &self.speed,
            &self.lane,
            &self.direction,
        ])
    }
}

impl Crud for FifteenMinuteVehicle {
    const COUNT_TABLE: &'static str = "tc_15minvolcount_new";

    fn prepare_insert(conn: &Connection) -> Result<Statement, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (recordnum, countdatetime, volume, cntdir, countlane) \
            VALUES (:1, :2, :3, :4, :5)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        stmt.execute(&[
            &self.recordnum,
            &NaiveDateTime::new(self.date, self.time.time()),
            &self.count,
            &self.direction,
            &self.lane,
        ])
    }
}

impl Crud for HourlyVehicle {
    const COUNT_TABLE: &'static str = "tc_volcount_new";

    fn prepare_insert(conn: &Connection) -> Result<Statement, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (recordnum, countdatetime, volume, countlane, cntdir) \
            VALUES (:1, :2, :3, :4, :5)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        stmt.execute(&[
            &self.recordnum,
            &self.datetime,
            &self.count,
            &self.lane,
            &self.direction,
        ])
    }
}

impl Crud for FifteenMinuteBicycle {
    const COUNT_TABLE: &'static str = "tc_bikecount_new";

    fn prepare_insert(conn: &Connection) -> Result<Statement, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (recordnum, countdatetime, volume, cntdir) \
            VALUES (:1, :2, :3, :4)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        stmt.execute(&[&self.recordnum, &self.datetime, &self.volume, &self.cntdir])
    }
}

impl Crud for FifteenMinutePedestrian {
    const COUNT_TABLE: &'static str = "tc_pedcount_new";

    fn prepare_insert(conn: &Connection) -> Result<Statement, oracle::Error> {
        let sql = &format!(
            "insert into {}
            (recordnum, countdatetime, volume, cntdir) \
            VALUES (:1, :2, :3, :4)",
            &Self::COUNT_TABLE,
        );
        conn.statement(sql).build()
    }

    fn insert(&self, stmt: &mut Statement) -> Result<(), oracle::Error> {
        stmt.execute(&[&self.recordnum, &self.datetime, &self.volume, &self.cntdir])
    }
}
