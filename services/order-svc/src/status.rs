use std::fmt;
use std::str::FromStr;

/// Lifecycle of an order in the MVP. Variant order matters: it is the
/// only allowed direction of change, which makes out-of-order or replayed
/// kitchen events harmless.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum OrderStatus {
    Accepted,
    Cooking,
    Cooked,
}

impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderStatus::Accepted => "accepted",
            OrderStatus::Cooking => "cooking",
            OrderStatus::Cooked => "cooked",
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
        match s {
            "accepted" => Ok(OrderStatus::Accepted),
            "cooking" => Ok(OrderStatus::Cooking),
            "cooked" => Ok(OrderStatus::Cooked),
            other => Err(UnknownStatus(other.to_string())),
        }
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
            advance(OrderStatus::Accepted, OrderStatus::Cooking),
            Some(OrderStatus::Cooking)
        );
        assert_eq!(
            advance(OrderStatus::Accepted, OrderStatus::Cooked),
            Some(OrderStatus::Cooked)
        );
        assert_eq!(advance(OrderStatus::Cooked, OrderStatus::Cooking), None);
        assert_eq!(advance(OrderStatus::Cooking, OrderStatus::Cooking), None);
    }

    #[test]
    fn string_roundtrip() {
        for s in [
            OrderStatus::Accepted,
            OrderStatus::Cooking,
            OrderStatus::Cooked,
        ] {
            assert_eq!(s.as_str().parse::<OrderStatus>(), Ok(s));
        }
        assert!("delivered".parse::<OrderStatus>().is_err());
    }
}
