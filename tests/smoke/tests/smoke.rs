use std::time::Duration;

use smoke::Stack;

#[tokio::test]
#[ignore = "needs a running stack: make up && make smoke"]
async fn order_reaches_cooked() -> anyhow::Result<()> {
    let stack = Stack::from_env();
    stack.wait_all_healthy(Duration::from_secs(120)).await?;
    let id = stack.create_order().await?;
    stack
        .wait_for_status(&id, "cooked", Duration::from_secs(60))
        .await
}

#[tokio::test]
#[ignore = "needs a running stack: make up && make smoke"]
async fn stopping_kitchen_stalls_orders_until_healed() -> anyhow::Result<()> {
    let stack = Stack::from_env();
    stack.wait_all_healthy(Duration::from_secs(120)).await?;

    let fault = stack.start_fault("kitchen-svc", "stop", 120).await?;
    let result = async {
        stack
            .wait_for_node_state("kitchen-svc", "broken", Duration::from_secs(15))
            .await?;
        assert_eq!(
            stack.node_state("order-svc").await?,
            "healthy",
            "producer must not depend on consumer"
        );
        assert_eq!(stack.node_state("kafka").await?, "healthy");

        let id = stack.create_order().await?;
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(
            stack.order_status(&id).await?,
            "accepted",
            "order must stall while the kitchen is down"
        );
        Ok::<_, anyhow::Error>(id)
    }
    .await;
    stack.stop_fault(fault).await?;
    let id = result?;

    stack
        .wait_for_node_state("kitchen-svc", "healthy", Duration::from_secs(60))
        .await?;
    stack
        .wait_for_status(&id, "cooked", Duration::from_secs(90))
        .await
}

#[tokio::test]
#[ignore = "needs a running stack: make up && make smoke"]
async fn stopping_redis_degrades_order_svc_but_menu_still_works() -> anyhow::Result<()> {
    let stack = Stack::from_env();
    stack.wait_all_healthy(Duration::from_secs(120)).await?;

    let fault = stack.start_fault("redis", "stop", 60).await?;
    let result = async {
        stack
            .wait_for_node_state("redis", "broken", Duration::from_secs(15))
            .await?;
        assert_eq!(stack.node_state("order-svc").await?, "degraded");
        assert_eq!(
            stack.node_state("gateway-1").await?,
            "healthy",
            "soft dependency stops after one hop"
        );
        let menu = stack.menu().await?;
        assert_eq!(menu.status(), 200);
        let body: serde_json::Value = menu.json().await?;
        assert_eq!(body["from_cache"], false);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    stack.stop_fault(fault).await?;
    result?;
    stack.wait_all_healthy(Duration::from_secs(60)).await
}

#[tokio::test]
#[ignore = "needs a running stack: make up && make smoke"]
async fn killing_one_gateway_replica_keeps_shop_available() -> anyhow::Result<()> {
    let stack = Stack::from_env();
    stack.wait_all_healthy(Duration::from_secs(120)).await?;

    let fault = stack.start_fault("gateway-1", "stop", 60).await?;
    let result = async {
        stack
            .wait_for_node_state("gateway-1", "broken", Duration::from_secs(15))
            .await?;
        assert_eq!(stack.node_state("traefik").await?, "degraded");
        tokio::time::sleep(Duration::from_secs(3)).await;
        for _ in 0..10 {
            assert_eq!(
                stack.menu().await?.status(),
                200,
                "LB must route around the dead replica"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    stack.stop_fault(fault).await?;
    result?;
    stack.wait_all_healthy(Duration::from_secs(60)).await
}

#[tokio::test]
#[ignore = "needs a running stack: make up && make smoke"]
async fn injected_payment_errors_open_the_circuit_breaker() -> anyhow::Result<()> {
    let stack = Stack::from_env();
    stack.wait_all_healthy(Duration::from_secs(120)).await?;

    let fault = stack.start_fault("payment-svc", "app-error", 120).await?;
    let result = async {
        // Loadgen keeps ordering, so the breaker trips on its own within a
        // few seconds; we only need to observe it.
        stack
            .wait_for_breaker("payment-svc", "open", Duration::from_secs(60))
            .await?;
        let resp = stack.create_order_raw().await?;
        assert_eq!(
            resp.status(),
            503,
            "orders fail fast while the breaker is open"
        );
        assert!(resp.text().await?.contains("circuit open"));
        assert_eq!(
            stack.node_state("order-svc").await?,
            "degraded",
            "erroring edge degrades the caller"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    stack.stop_fault(fault).await?;
    result?;
    // Breaker half-opens after 20 s and closes on the first good probe.
    stack
        .wait_for_breaker("payment-svc", "closed", Duration::from_secs(90))
        .await?;
    stack.wait_all_healthy(Duration::from_secs(60)).await
}

#[tokio::test]
#[ignore = "needs a running stack: make up && make smoke"]
async fn network_latency_on_order_svc_degrades_gateways_by_metrics() -> anyhow::Result<()> {
    let stack = Stack::from_env();
    stack.wait_all_healthy(Duration::from_secs(120)).await?;

    let fault = stack.start_fault("order-svc", "latency", 120).await?;
    let result = async {
        stack
            .wait_for_node_state("order-svc", "broken", Duration::from_secs(15))
            .await?;
        // 800 ms netem on every hop out of order-svc pushes gateway → order-svc
        // p95 past the 1 s threshold; that is the metric-driven path.
        stack
            .wait_for_edge_p95_above("gateway-1", "order-svc", 1000.0, Duration::from_secs(90))
            .await?;
        let menu = stack.menu().await?;
        assert_eq!(menu.status(), 200, "slow is not down");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    stack.stop_fault(fault).await?;
    result?;
    stack.wait_all_healthy(Duration::from_secs(90)).await
}

#[tokio::test]
#[ignore = "needs a running stack: make up && make smoke"]
async fn stopping_courier_stalls_orders_at_cooked() -> anyhow::Result<()> {
    let stack = Stack::from_env();
    stack.wait_all_healthy(Duration::from_secs(120)).await?;

    let fault = stack.start_fault("courier-svc", "stop", 120).await?;
    let result = async {
        stack
            .wait_for_node_state("courier-svc", "broken", Duration::from_secs(15))
            .await?;
        let id = stack.create_order().await?;
        stack
            .wait_for_status(&id, "cooked", Duration::from_secs(60))
            .await?;
        tokio::time::sleep(Duration::from_secs(15)).await;
        assert_eq!(
            stack.order_status(&id).await?,
            "cooked",
            "no courier, no delivery"
        );
        Ok::<_, anyhow::Error>(id)
    }
    .await;
    stack.stop_fault(fault).await?;
    let id = result?;
    stack
        .wait_for_status(&id, "delivered", Duration::from_secs(90))
        .await
}
