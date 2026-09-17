use std::fmt;
use std::str::FromStr;

/// Lifecycle of an order. Variant order matters: it is the only allowed
/// direction of change, which makes out-of-order or replayed events from
/// the kitchen and courier harmless.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum OrderStatus {
    Accepted,
    Paid,
    Cooking,
    Cooked,
    Delivering,
    Delivered,
}

impl OrderStatus {
    pub const ALL: [OrderStatus; 6] = [
        OrderStatus::Accepted,
        OrderStatus::Paid,
        OrderStatus::Cooking,
        OrderStatus::Cooked,
        OrderStatus::Delivering,
        OrderStatus::Delivered,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            OrderStatus::Accepted => "accepted",
            OrderStatus::Paid => "paid",
            OrderStatus::Cooking => "cooking",
            OrderStatus::Cooked => "cooked",
            OrderStatus::Delivering => "delivering",
            OrderStatus::Delivered => "delivered",
        }
    }
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OrderStatus {
    type Err = UnknownStatus;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        OrderStatus::ALL
            .into_iter()
            .find(|v| v.as_str() == s)
            .ok_or_else(|| UnknownStatus(s.to_string()))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct UnknownStatus(pub String);

impl fmt::Display for UnknownStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown order status {:?}", self.0)
    }
}

impl std::error::Error for UnknownStatus {}

/// Returns the new status only when it moves the order forward.
pub fn advance(current: OrderStatus, incoming: OrderStatus) -> Option<OrderStatus> {
    (incoming > current).then_some(incoming)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn moves_forward_only() {
        assert_eq!(
            advance(OrderStatus::Accepted, OrderStatus::Paid),
            Some(OrderStatus::Paid)
        );
        assert_eq!(
            advance(OrderStatus::Paid, OrderStatus::Cooked),
            Some(OrderStatus::Cooked)
        );
        assert_eq!(
            advance(OrderStatus::Delivered, OrderStatus::Delivering),
            None
        );
        assert_eq!(advance(OrderStatus::Cooking, OrderStatus::Cooking), None);
    }

    #[test]
    fn string_roundtrip_for_every_status() {
        for s in OrderStatus::ALL {
            assert_eq!(s.as_str().parse::<OrderStatus>(), Ok(s));
        }
        assert!("refunded".parse::<OrderStatus>().is_err());
    }
}
