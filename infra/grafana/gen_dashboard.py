import json, sys
DS = {"type": "prometheus", "uid": "prometheus"}
A_COLOR, B_COLOR = "#4e79a7", "#f28e2b"
_id = [0]
def nid():
    _id[0] += 1
    return _id[0]

def tgt(expr, legend, ref):
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref, "interval": "5s"}

def targets(items):
    return [tgt(e, l, chr(65 + i)) for i, (e, l) in enumerate(items)]

def row(title, y):
    return {"type": "row", "title": title, "id": nid(), "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}

def color_override(name_regex, color):
    return {"matcher": {"id": "byRegexp", "options": name_regex},
            "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]}

def ts(title, x, y, w, h, items, unit="short", overrides=None, desc="", stack=False, minv=None):
    custom = {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 12, "showPoints": "never",
              "spanNulls": False, "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    d = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}}
    if minv is not None:
        d["min"] = minv
    return {"type": "timeseries", "title": title, "id": nid(), "description": desc, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets(items),
            "fieldConfig": {"defaults": d, "overrides": overrides or []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}

def stat(title, x, y, w, h, expr, desc=""):
    return {"type": "stat", "title": title, "id": nid(), "description": desc, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": [tgt(expr, "", "A")],
            "fieldConfig": {"defaults": {
                "mappings": [{"type": "value", "options": {
                    "0": {"text": "DOWN", "color": "red", "index": 0},
                    "1": {"text": "UP", "color": "green", "index": 1}}}],
                "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}]},
                "noValue": "DOWN", "color": {"mode": "thresholds"}}, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "background", "graphMode": "none", "textMode": "value", "justifyMode": "center"}}

HTTP_CODES = [color_override("2..", "green"), color_override("4..", "yellow"), color_override("5..", "red")]
NOT_INFRA = 'route!~".*/(metrics|health|live)"'

panels = []
y = 0
panels.append(row("Global router (HAProxy :8080) and k6 client view", y)); y += 1
panels.append(stat("Router sees region A", 0, y, 4, 8, 'haproxy_server_up{proxy="be_active_active",server="region-a"}',
                   "1 while /health of region A answers 200 within the router's checks (fall 2 / rise 2, every 1s)."))
panels.append(stat("Router sees region B", 4, y, 4, 8, 'haproxy_server_up{proxy="be_active_active",server="region-b"}'))
reg_over = [color_override("a", A_COLOR), color_override("b", B_COLOR), color_override("none", "red")]
panels.append(ts("Client traffic by region that answered (k6, req/s)", 8, y, 8, 8,
                 [('sum by (region)(rate(k6_requests_total[15s]))', "{{region}}")], "reqps", reg_over,
                 "Which region served the requests. A region going down makes its line drop to 0 and the other one double."))
panels.append(ts("Client failures (k6: 5xx or no response, req/s)", 16, y, 8, 8,
                 [('sum by (region,op)(rate(k6_requests_total{status=~"0|5.."}[15s]))', "{{op}} / {{region}} / {{status}}")], "reqps",
                 None, "Requests that failed from the client's point of view. The gap during a failover shows up here.")); y += 8

panels.append(row("Cross-region replication (k6 verifier: write via router, poll the OTHER region directly)", y)); y += 1
panels.append(ts("Replication visibility p95 (ms)", 0, y, 12, 7,
                 [('max by (from,to)(k6_replication_lag_ms_p95)', "{{from}} -> {{to}} p95")], "ms", None,
                 "Time until a write made in one region is readable in the other. Grows with partitions / outages (this is your RPO in practice)."))
panels.append(ts("Replication timeouts (writes not visible in the other region within the timeout, /s)", 12, y, 12, 7,
                 [('sum by (from,to)(rate(k6_replication_timeouts_total[30s]))', "{{from}} -> {{to}}")], "ops", None)); y += 7

panels.append(row("Regions: A on the left, B on the right", y)); y += 1

def region_block(r, x0, y0):
    out = []
    lab = f'region="{r}"'
    T = r.upper()
    out.append(stat(f"{T} · service", x0, y0, 3, 4, f'up{{job="customer-service",{lab}}}'))
    out.append(stat(f"{T} · postgres", x0 + 3, y0, 3, 4, f'dependency_up{{{lab},dependency="db"}}'))
    out.append(stat(f"{T} · kafka (local)", x0 + 6, y0, 3, 4, f'dependency_up{{{lab},dependency="kafka"}}'))
    out.append(stat(f"{T} · kafka of the OTHER region (WAN)", x0 + 9, y0, 3, 4, f'dependency_up{{{lab},dependency="remote_kafka"}}',
                    "Goes DOWN on a network partition or when the other region is lost."))
    y1 = y0 + 4
    out.append(ts(f"{T} · requests/s by status code", x0, y1, 6, 8,
                  [(f'sum by (code)(rate(http_requests_total{{{lab},{NOT_INFRA}}}[15s]))', "{{code}}")], "reqps", HTTP_CODES))
    out.append(ts(f"{T} · latency p95", x0 + 6, y1, 6, 8,
                  [(f'histogram_quantile(0.95, sum by (le)(rate(http_request_duration_seconds_bucket{{{lab},{NOT_INFRA}}}[30s])))', "p95")],
                  "s", None))
    y2 = y1 + 8
    out.append(ts(f"{T} · customers by publish status", x0, y2, 6, 8,
                  [(f'customers_by_status{{{lab}}}', "{{status}}")], "short",
                  [color_override("PUBLISHED", "green"), color_override("PENDING", "yellow"), color_override("PUBLISH_FAILED", "red")],
                  "PENDING + PUBLISH_FAILED is the outbox backlog. Kafka down => PUBLISH_FAILED grows; Kafka back => the relay drains it."))
    out.append(ts(f"{T} · events published to {r}.customers (/s)", x0 + 6, y2, 6, 8,
                  [(f'sum by (result)(rate(customer_events_published_total{{{lab}}}[15s]))', "{{result}}")], "ops",
                  [color_override("ok", "green"), color_override("error", "red")]))
    y3 = y2 + 8
    other = "b" if r == "a" else "a"
    out.append(ts(f"{T} · events replicated in from region {other.upper()} (/s)", x0, y3, 6, 8,
                  [(f'sum by (origin_region,result)(rate(replication_applied_total{{{lab}}}[15s]))', "from {{origin_region}}: {{result}}")], "ops"))
    out.append(ts(f"{T} · replication lag (s)", x0 + 6, y3, 6, 8,
                  [(f'replication_lag_seconds{{{lab}}}', "lag")], "s", None,
                  "Age of the last event applied here. Spikes after an outage while the backlog is drained.", minv=0))
    return out

panels += region_block("a", 0, y)
panels += region_block("b", 12, y)

dash = {
    "uid": "multi-region", "title": "Multi-Region · Customers", "tags": ["multi-region", "study"],
    "timezone": "browser", "schemaVersion": 39, "version": 1, "editable": True, "graphTooltip": 1,
    "refresh": "5s", "time": {"from": "now-5m", "to": "now"}, "timepicker": {"refresh_intervals": ["5s", "10s", "30s"]},
    "templating": {"list": []}, "annotations": {"list": []}, "links": [], "panels": panels,
}
json.dump(dash, open(sys.argv[1], "w"), indent=2)
print("panels:", len(panels))
