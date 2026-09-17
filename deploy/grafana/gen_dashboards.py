#!/usr/bin/env python3
"""Generates the provisioned Grafana dashboards.

Dashboard JSON is 90% boilerplate; keeping the queries in one short Python
file makes them reviewable. Run after editing: python3 deploy/grafana/gen_dashboards.py
"""
import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
DS = {"type": "prometheus", "uid": "prom"}


def target(expr, legend):
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": legend[:1] or "A"}


def panel(pid, title, targets, x, y, w=12, h=8, kind="timeseries", unit=None):
    p = {
        "id": pid,
        "type": kind,
        "title": title,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [dict(t, refId=chr(65 + i)) for i, t in enumerate(targets)],
        "fieldConfig": {"defaults": {"unit": unit} if unit else {}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}} if kind == "timeseries" else {},
    }
    return p


def dashboard(uid, title, panels, templating=None):
    return {
        "uid": uid,
        "title": title,
        "schemaVersion": 39,
        "version": 1,
        "editable": False,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "timezone": "browser",
        "tags": ["chaos-panel"],
        "templating": {"list": templating or []},
        "panels": panels,
    }


overview = dashboard("overview", "Chaos Panel · Overview", [
    panel(1, "People online", [target("sum(loadgen_active_users) + (sum(gateway_sse_connections) or vector(0))", "online")], 0, 0, 6, 4, "stat"),
    panel(2, "Orders / min", [target("sum(increase(orders_created_total[1m]))", "orders")], 6, 0, 6, 4, "stat"),
    panel(3, "Kitchen backlog (Kafka lag)", [target("sum(kafka_consumergroup_lag)", "lag")], 12, 0, 6, 4, "stat"),
    panel(4, "Orders in kitchen / in transit", [target("sum(kitchen_orders_in_progress)", "cooking"), target("sum(courier_orders_in_transit)", "delivering")], 18, 0, 6, 4, "stat"),
    panel(11, "Order lead time, 5 min avg (ClickHouse)", [target("analytics_lead_time_seconds", "lead")], 0, 28, 6, 4, "stat", unit="s"),
    panel(12, "Payment circuit breaker (0 closed, 1 open, 2 half-open)", [target("max(circuit_breaker_state{name=\"payment-svc\"})", "breaker")], 6, 28, 6, 4, "stat"),
    panel(13, "Payments per minute", [target("sum by (outcome) (rate(payments_total[1m])) * 60", "{{outcome}}")], 12, 28, 12, 8, unit="opm"),
    panel(14, "Delivered per 5 min (ClickHouse)", [target("analytics_delivered_5m", "delivered")], 0, 32, 6, 4, "stat"),
    panel(5, "Requests per second by edge", [target("sum by (src, dst) (rate(edge_requests_total[30s]))", "{{src}} → {{dst}}")], 0, 4, unit="reqps"),
    panel(6, "Errors per second by edge", [target("sum by (src, dst) (rate(edge_requests_total{outcome=\"error\"}[30s]))", "{{src}} → {{dst}}")], 12, 4, unit="reqps"),
    panel(7, "p95 latency by edge", [target("histogram_quantile(0.95, sum by (le, src, dst) (rate(edge_request_duration_seconds_bucket[1m])))", "{{src}} → {{dst}}")], 0, 12, unit="s"),
    panel(8, "Customer outcomes (loadgen)", [target("sum by (outcome) (rate(loadgen_orders_total[1m])) * 60", "{{outcome}}")], 12, 12, unit="opm"),
    panel(9, "Edge traffic by HTTP code (Traefik)", [target("sum by (code) (rate(traefik_service_requests_total{service=~\"gateway.*\"}[30s]))", "{{code}}")], 0, 20, unit="reqps"),
    panel(10, "Kafka lag by consumer group", [target("sum by (consumergroup) (kafka_consumergroup_lag)", "{{consumergroup}}")], 12, 20),
])

service_var = {
    "name": "service",
    "type": "query",
    "datasource": DS,
    "query": {"query": "label_values(edge_requests_total, service)", "refId": "svc"},
    "definition": "label_values(edge_requests_total, service)",
    "refresh": 2,
    "current": {"text": "order-svc", "value": "order-svc"},
}
service = dashboard("service", "Chaos Panel · Service", [
    panel(1, "Outgoing calls from $service", [target("sum by (dst, outcome) (rate(edge_requests_total{service=\"$service\"}[30s]))", "→ {{dst}} ({{outcome}})")], 0, 0, unit="reqps"),
    panel(2, "Incoming calls to $service", [target("sum by (src, outcome) (rate(edge_requests_total{dst=\"$service\"}[30s]))", "{{src}} → ({{outcome}})")], 12, 0, unit="reqps"),
    panel(3, "p95 latency of outgoing calls", [target("histogram_quantile(0.95, sum by (le, dst) (rate(edge_request_duration_seconds_bucket{service=\"$service\"}[1m])))", "→ {{dst}}")], 0, 8, unit="s"),
    panel(5, "p95 latency of incoming calls", [target("histogram_quantile(0.95, sum by (le, src) (rate(edge_request_duration_seconds_bucket{dst=\"$service\"}[1m])))", "{{src}} →")], 0, 16, unit="s"),
    panel(4, "Error ratio of outgoing calls", [target("sum(rate(edge_requests_total{service=\"$service\",outcome=\"error\"}[1m])) / sum(rate(edge_requests_total{service=\"$service\"}[1m]))", "errors")], 12, 8, unit="percentunit"),
], [service_var])

kafka = dashboard("kafka", "Chaos Panel · Kafka", [
    panel(1, "Consumer lag by group", [target("sum by (consumergroup) (kafka_consumergroup_lag)", "{{consumergroup}}")], 0, 0),
    panel(2, "Messages in per topic", [target("sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~\"orders.*\"}[1m]))", "{{topic}}")], 12, 0, unit="mps"),
    panel(3, "Consumer group members", [target("kafka_consumergroup_members", "{{consumergroup}}")], 0, 8),
    panel(4, "Orders in kitchen", [target("sum(kitchen_orders_in_progress)", "cooking")], 12, 8),
])

OUT.mkdir(exist_ok=True)
for d in (overview, service, kafka):
    (OUT / f"{d['uid']}.json").write_text(json.dumps(d, ensure_ascii=False, indent=1) + "\n")
    print("wrote", d["uid"])
