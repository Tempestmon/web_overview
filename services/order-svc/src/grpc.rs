use std::collections::HashMap;

use proto::order::order_service_server::OrderService;
use proto::order::{
    CreateOrderRequest, GetMenuRequest, GetOrderRequest, Menu, MenuItem, Order, OrderLine,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::kafka::Producer;
use crate::menu_cache::MenuCache;
use crate::repo::{Line, MenuRow, NewOrder, OrderRow, Repo};

pub struct OrderSvc {
    pub repo: Repo,
    pub cache: MenuCache,
    pub producer: Producer,
}

#[tonic::async_trait]
impl OrderService for OrderSvc {
    async fn get_menu(&self, _req: Request<GetMenuRequest>) -> Result<Response<Menu>, Status> {
        if let Some(items) = self.cache.get().await {
            return Ok(Response::new(menu(items, true)));
        }
        let items = self.repo.list_menu().await.map_err(db_unavailable)?;
        self.cache.put(&items).await;
        Ok(Response::new(menu(items, false)))
    }

    async fn create_order(
        &self,
        req: Request<CreateOrderRequest>,
    ) -> Result<Response<Order>, Status> {
        let req = req.into_inner();
        let lines = validate_lines(&req.lines)?;
        let menu = self.repo.list_menu().await.map_err(db_unavailable)?;
        let total_cents = price(&menu, &lines)?;
        let order = NewOrder {
            id: Uuid::new_v4(),
            client_id: req.client_id,
            total_cents,
            lines,
        };
        let row = self
            .repo
            .insert_order(order)
            .await
            .map_err(db_unavailable)?;

        // An order the kitchen never hears about would sit in "accepted" forever.
        // Roll it back so the caller sees a clean failure instead of a ghost order.
        if let Err(err) = self.producer.publish_created(row.id).await {
            tracing::warn!(%err, order_id = %row.id, "rolling back order: kafka unavailable");
            let _ = self.repo.delete_order(row.id).await;
            return Err(Status::unavailable("kafka unavailable"));
        }
        metrics::counter!("orders_created_total").increment(1);
        Ok(Response::new(to_proto(row)))
    }

    async fn get_order(&self, req: Request<GetOrderRequest>) -> Result<Response<Order>, Status> {
        let id = Uuid::parse_str(&req.into_inner().id)
            .map_err(|_| Status::invalid_argument("bad order id"))?;
        let row = self.repo.get_order(id).await.map_err(db_unavailable)?;
        row.map(|r| Response::new(to_proto(r)))
            .ok_or_else(|| Status::not_found("order not found"))
    }
}

fn validate_lines(lines: &[OrderLine]) -> Result<Vec<Line>, Status> {
    if lines.is_empty() {
        return Err(Status::invalid_argument("order has no lines"));
    }
    lines
        .iter()
        .map(|l| {
            if l.qty <= 0 || l.qty > 20 {
                return Err(Status::invalid_argument("qty must be within 1..=20"));
            }
            Ok(Line {
                item_id: l.item_id,
                qty: l.qty,
            })
        })
        .collect()
}

fn price(menu: &[MenuRow], lines: &[Line]) -> Result<i64, Status> {
    let prices: HashMap<i64, i64> = menu.iter().map(|m| (m.id, m.price_cents)).collect();
    lines.iter().try_fold(0i64, |acc, l| {
        let unit = prices
            .get(&l.item_id)
            .ok_or_else(|| Status::invalid_argument(format!("unknown item {}", l.item_id)))?;
        Ok(acc + unit * i64::from(l.qty))
    })
}

fn menu(items: Vec<MenuRow>, from_cache: bool) -> Menu {
    Menu {
        items: items
            .into_iter()
            .map(|i| MenuItem {
                id: i.id,
                name: i.name,
                price_cents: i.price_cents,
            })
            .collect(),
        from_cache,
    }
}

fn to_proto(row: OrderRow) -> Order {
    Order {
        id: row.id.to_string(),
        status: row.status,
        total_cents: row.total_cents,
        lines: row
            .lines
            .0
            .into_iter()
            .map(|l| OrderLine {
                item_id: l.item_id,
                qty: l.qty,
            })
            .collect(),
        created_at: row.created_at.to_rfc3339(),
        updated_at: row.updated_at.to_rfc3339(),
    }
}

fn db_unavailable(err: sqlx::Error) -> Status {
    tracing::warn!(%err, "database error");
    Status::unavailable("database unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn menu_rows() -> Vec<MenuRow> {
        vec![
            MenuRow {
                id: 1,
                name: "a".into(),
                price_cents: 100,
            },
            MenuRow {
                id: 2,
                name: "b".into(),
                price_cents: 250,
            },
        ]
    }

    #[test]
    fn prices_sum_over_lines() {
        let lines = vec![Line { item_id: 1, qty: 2 }, Line { item_id: 2, qty: 1 }];
        assert_eq!(price(&menu_rows(), &lines).unwrap(), 450);
    }

    #[test]
    fn unknown_item_is_invalid_argument() {
        let lines = vec![Line { item_id: 9, qty: 1 }];
        assert_eq!(
            price(&menu_rows(), &lines).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn rejects_empty_and_bad_qty() {
        assert!(validate_lines(&[]).is_err());
        assert!(validate_lines(&[OrderLine { item_id: 1, qty: 0 }]).is_err());
        assert!(validate_lines(&[OrderLine { item_id: 1, qty: 3 }]).is_ok());
    }
}
