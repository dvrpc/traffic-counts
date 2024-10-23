//! Various implementations for use with an Oracle database.
use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime};
use log::Level;
use oracle::{sql_type::FromSql, Error as OracleError, RowValue, SqlValue};

use crate::{
    db::ImportLogEntry, denormalize::NonNormalVolCount, LaneDirection, Metadata,
    OptionLaneDirection, OptionRoadDirection, RoadDirection,
};

impl RowValue for Metadata {
    fn get(row: &oracle::Row) -> oracle::Result<Self> {
        let amending = row.get("amending")?;
        let ampeak = row.get("ampeak")?;
        let bikepeddesc = row.get("bikepeddesc")?;
        let bikepedfacility = row.get("bikepedfacility")?;
        let bikepedgroup = row.get("bikepedgroup")?;
        let cntdir = row.get("cntdir")?;
        let comments = row.get("comments")?;
        let count_type = row.get("type")?;
        let counterid = row.get("counterid")?;
        let createheaderdate = row.get::<&str, Option<NaiveDate>>("createheaderdate")?;
        let datelastcounted = row.get::<&str, Option<NaiveDate>>("datelastcounted")?;
        let description = row.get("description")?;
        let fc = row.get("fc")?;
        let fromlmt = row.get("fromlmt")?;
        let importdatadate = row.get::<&str, Option<NaiveDate>>("importdatadate")?;
        let indir = row.get("indir")?;
        let isurban = {
            let isurban: String = row.get("isurban")?;
            if isurban == *"Y" {
                Some(true)
            } else {
                Some(false)
            }
        };
        let latitude = row.get("latitude")?;
        let longitude = row.get("longitude")?;
        let mcd = row.get("mcd")?;
        let mp = row.get("mp")?;
        let offset = row.get("offset")?;
        let outdir = row.get("outdir")?;
        let pmending = row.get("pmending")?;
        let pmpeak = row.get("pmpeak")?;
        let prj = row.get("prj")?;
        let program = row.get("program")?;
        let record_num = row.get("recordnum")?;
        let rdprefix = row.get("rdprefix")?;
        let rdsuffix = row.get("rdsuffix")?;
        let road = row.get("road")?;
        let route = row.get("route")?;
        let seg = row.get("seg")?;
        let sidewalk = row.get("sidewalk")?;
        let speed_limit = row.get("speedlimit")?;
        let source = row.get("source")?;
        let sr = row.get("sr")?;
        let sri = row.get("sri")?;
        let stationid = row.get("stationid")?;
        let technician = row.get("takenby")?;
        let tolmt = row.get("tolmt")?;
        let trafdir = row.get("trafdir")?;
        let x = row.get("x")?;
        let y = row.get("y")?;

        let record = Metadata {
            amending,
            ampeak,
            bikepeddesc,
            bikepedgroup,
            bikepedfacility,
            cntdir,
            comments,
            count_type,
            counterid,
            createheaderdate,
            datelastcounted,
            description,
            fc,
            fromlmt,
            importdatadate,
            indir,
            isurban,
            latitude,
            longitude,
            mcd,
            mp,
            offset,
            outdir,
            pmending,
            pmpeak,
            prj,
            program,
            record_num,
            rdprefix,
            rdsuffix,
            road,
            route,
            seg,
            sidewalk,
            speed_limit,
            source,
            sr,
            sri,
            stationid,
            technician,
            tolmt,
            trafdir,
            x,
            y,
        };
        Ok(record)
    }
}

impl RowValue for ImportLogEntry {
    fn get(row: &oracle::Row) -> oracle::Result<Self> {
        let record_num = row.get("recordnum")?;
        let msg: String = row.get("message")?;
        let level: String = row.get("log_level")?;
        let level = Level::from_str(level.as_str()).unwrap();
        let datetime: NaiveDateTime = row.get("datetime")?;
        let mut log_record = ImportLogEntry::new(record_num, msg, level);
        log_record.datetime = Some(datetime);
        Ok(log_record)
    }
}

impl FromSql for LaneDirection {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match LaneDirection::from_string(val.to_string()) {
            Ok(v) => Ok(v),
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl FromSql for OptionLaneDirection {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match val.is_null() {
            Ok(true) => Ok(OptionLaneDirection(None)),
            Ok(false) => match LaneDirection::from_string(val.to_string()) {
                Ok(v) => Ok(OptionLaneDirection(Some(v))),
                Err(e) => Err(OracleError::ParseError(Box::new(e))),
            },
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl FromSql for RoadDirection {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match RoadDirection::from_string(val.to_string()) {
            Ok(v) => Ok(v),
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl FromSql for OptionRoadDirection {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match val.is_null() {
            Ok(true) => Ok(OptionRoadDirection(None)),
            Ok(false) => match RoadDirection::from_string(val.to_string()) {
                Ok(v) => Ok(OptionRoadDirection(Some(v))),
                Err(e) => Err(OracleError::ParseError(Box::new(e))),
            },
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl RowValue for NonNormalVolCount {
    fn get(row: &oracle::Row) -> oracle::Result<Self> {
        Ok(NonNormalVolCount {
            record_num: row.get("recordnum")?,
            date: row.get("date")?,
            direction: row.get("cntdir")?,
            lane: row.get("countlane")?,
            setflag: row.get("")?,
            totalcount: row.get("totalcount")?,
            weather: None,
            am12: row.get("am12")?,
            am1: row.get("am1")?,
            am2: row.get("am2")?,
            am3: row.get("am3")?,
            am4: row.get("am4")?,
            am5: row.get("am5")?,
            am6: row.get("am6")?,
            am7: row.get("am7")?,
            am8: row.get("am8")?,
            am9: row.get("am9")?,
            am10: row.get("am10")?,
            am11: row.get("am11")?,
            pm12: row.get("pm12")?,
            pm1: row.get("pm1")?,
            pm2: row.get("pm2")?,
            pm3: row.get("pm3")?,
            pm4: row.get("pm4")?,
            pm5: row.get("pm5")?,
            pm6: row.get("pm6")?,
            pm7: row.get("pm7")?,
            pm8: row.get("pm8")?,
            pm9: row.get("pm9")?,
            pm10: row.get("pm10")?,
            pm11: row.get("pm11")?,
        })
    }
}
