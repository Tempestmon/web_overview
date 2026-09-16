use chrono::{DateTime, Utc};
use common::Edge;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::status::{self, OrderStatus};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Line {
    pub item_id: i64,
    pub qty: i32,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct MenuRow {
    pub id: i64,
    pub name: String,
    pub price_cents: i64,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct OrderRow {
    pub id: Uuid,
    pub status: String,
    pub total_cents: i64,
    pub lines: sqlx::types::Json<Vec<Line>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct NewOrder {
    pub id: Uuid,
    pub client_id: String,
    pub total_cents: i64,
    pub lines: Vec<Line>,
}

#[derive(Clone)]
pub struct Repo {
    pool: PgPool,
    edge: Edge,
}

impl Repo {
    pub fn new(pool: PgPool, service_id: &str) -> Self {
        Self {
            pool,
            edge: Edge::new(service_id, "postgres", "sql"),
        }
    }

    pub async fn ping(&self) -> bool {
        self.edge
            .observe(sqlx::query("SELECT 1").execute(&self.pool))
            .await
            .is_ok()
    }

    pub async fn list_menu(&self) -> sqlx::Result<Vec<MenuRow>> {
        self.edge
            .observe(
                sqlx::query_as::<_, MenuRow>(
                    "SELECT id, name, price_cents FROM menu_items ORDER BY id",
                )
                .fetch_all(&self.pool),
            )
            .await
    }

    pub async fn insert_order(&self, order: NewOrder) -> sqlx::Result<OrderRow> {
        self.edge
            .observe(
                sqlx::query_as::<_, OrderRow>(
                    "INSERT INTO orders (id, client_id, status, total_cents, lines)
                     VALUES ($1, $2, $3, $4, $5)
                     RETURNING id, status, total_cents, lines, created_at, updated_at",
                )
                .bind(order.id)
                .bind(&order.client_id)
                .bind(OrderStatus::Accepted.as_str())
                .bind(order.total_cents)
                .bind(sqlx::types::Json(order.lines))
                .fetch_one(&self.pool),
            )
            .await
    }

    pub async fn delete_order(&self, id: Uuid) -> sqlx::Result<()> {
        self.edge
            .observe(
                sqlx::query("DELETE FROM orders WHERE id = $1")
                    .bind(id)
                    .execute(&self.pool),
            )
            .await?;
        Ok(())
    }

    pub async fn get_order(&self, id: Uuid) -> sqlx::Result<Option<OrderRow>> {
        self.edge
            .observe(
                sqlx::query_as::<_, OrderRow>(
                    "SELECT id, status, total_cents, lines, created_at, updated_at
                     FROM orders WHERE id = $1",
                )
                .bind(id)
                .fetch_optional(&self.pool),
            )
            .await
    }

    /// Optimistic monotonic update: the row moves only if nobody advanced it
    /// past `incoming` in the meantime. Returns whether a change happened.
    pub async fn advance_status(&self, id: Uuid, incoming: OrderStatus) -> sqlx::Result<bool> {
        let Some(row) = self.get_order(id).await? else {
            return Ok(false);
        };
        let current: OrderStatus = row
            .status
            .parse()
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        let Some(next) = status::advance(current, incoming) else {
            return Ok(false);
        };
        let result = self
            .edge
            .observe(
                sqlx::query("UPDATE orders SET status = $1, updated_at = now() WHERE id = $2 AND status = $3")
                    .bind(next.as_str())
                    .bind(id)
                    .bind(current.as_str())
                    .execute(&self.pool),
            )
            .await?;
        Ok(result.rows_affected() == 1)
    }
}
