//! Pure translation of Kafka messages into ClickHouse rows, kept free of
//! I/O so it can be tested without a broker or a database.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

pub const TOPIC_CREATED: &str = "orders.created";
pub const TOPIC_KITCHEN_EVENTS: &str = "orders.kitchen-events";
pub const TOPIC_DELIVERY_EVENTS: &str = "orders.delivery-events";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub order_id: Uuid,
    pub status: String,
    pub ts: DateTime<Utc>,
}

#[derive(Deserialize)]
struct Created {
    order_id: Uuid,
}

#[derive(Deserialize)]
struct StatusEvent {
    order_id: Uuid,
    status: String,
}

/// `orders.created` carries no status because creation *is* the status;
/// every other topic spells it out.
pub fn row_from_message(topic: &str, payload: &[u8], ts: DateTime<Utc>) -> anyhow::Result<Row> {
    match topic {
        TOPIC_CREATED => {
            let created: Created = serde_json::from_slice(payload)?;
            Ok(Row {
                order_id: created.order_id,
                status: "accepted".to_string(),
                ts,
            })
        }
        TOPIC_KITCHEN_EVENTS | TOPIC_DELIVERY_EVENTS => {
            let event: StatusEvent = serde_json::from_slice(payload)?;
            Ok(Row {
                order_id: event.order_id,
                status: event.status,
                ts,
            })
        }
        other => anyhow::bail!("unexpected topic {other}"),
    }
}

/// One `JSONEachRow` line. ClickHouse's `DateTime64(3)` accepts
/// `YYYY-MM-DD HH:MM:SS.mmm` without a timezone suffix (server is UTC).
pub fn json_each_row_line(row: &Row) -> String {
    serde_json::json!({
        "order_id": row.order_id,
        "status": row.status,
        "ts": row.ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 17, 12, 34, 56).unwrap() + chrono::Duration::milliseconds(789)
    }

    #[test]
    fn created_message_becomes_accepted_row() {
        let id = Uuid::new_v4();
        let payload = format!(r#"{{"order_id":"{id}"}}"#);
        let row = row_from_message(TOPIC_CREATED, payload.as_bytes(), ts()).unwrap();
        assert_eq!(
            row,
            Row {
                order_id: id,
                status: "accepted".into(),
                ts: ts()
            }
        );
    }

    #[test]
    fn kitchen_and_delivery_events_keep_their_status() {
        let id = Uuid::new_v4();
        let payload = format!(r#"{{"order_id":"{id}","status":"cooking"}}"#);
        let row = row_from_message(TOPIC_KITCHEN_EVENTS, payload.as_bytes(), ts()).unwrap();
        assert_eq!(row.status, "cooking");
        let payload = format!(r#"{{"order_id":"{id}","status":"delivered"}}"#);
        let row = row_from_message(TOPIC_DELIVERY_EVENTS, payload.as_bytes(), ts()).unwrap();
        assert_eq!(row.status, "delivered");
    }

    #[test]
    fn garbage_and_unknown_topics_are_errors() {
        assert!(row_from_message(TOPIC_CREATED, b"not json", ts()).is_err());
        assert!(row_from_message(TOPIC_KITCHEN_EVENTS, b"{}", ts()).is_err());
        assert!(row_from_message("orders.unknown", b"{}", ts()).is_err());
    }

    #[test]
    fn json_each_row_uses_clickhouse_datetime_format() {
        let row = Row {
            order_id: Uuid::nil(),
            status: "cooked".into(),
            ts: ts(),
        };
        assert_eq!(
            json_each_row_line(&row),
            r#"{"order_id":"00000000-0000-0000-0000-000000000000","status":"cooked","ts":"2026-09-17 12:34:56.789"}"#
        );
    }
}
