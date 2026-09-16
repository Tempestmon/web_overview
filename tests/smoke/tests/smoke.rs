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
