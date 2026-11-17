import json
import socket
import sqlite3
import time

import pandas as pd
import plotly.express as px
import streamlit as st


def render_live(root):
    st.title("Live Stream")
    st.caption(
        "Kafka → Spark Structured Streaming → saved MLlib pipeline "
        "→ predictions topic + local dashboard database"
    )
    st.info(
        "Historical feature replay over actual Kafka streaming. "
        "Scores are uncalibrated; alerts use the saved validation threshold."
    )

    @st.fragment(run_every=3)
    def monitor():
        now = time.time()
        folder = root / "outputs/live"
        database = folder / "predictions.db"

        try:
            status = json.loads(
                (folder / "status.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError):
            status = {}

        heartbeat = float(status.get("heartbeat", 0))
        age = now - heartbeat if heartbeat else None
        active = (
            status.get("active", False)
            and age is not None
            and age < 20
        )

        try:
            with socket.create_connection(("127.0.0.1", 9092), timeout=1):
                reachable = True
        except OSError:
            reachable = False

        left, right = st.columns(2)
        with left:
            if reachable:
                st.success("Kafka TCP port reachable · 127.0.0.1:9092")
            else:
                st.error("Kafka TCP port unavailable")
        with right:
            if active:
                st.success("Spark consumer heartbeat fresh")
            elif age is None:
                st.warning("Consumer status not available")
            elif status.get("active"):
                st.warning("Consumer heartbeat stale — inspect its terminal")
            else:
                st.warning("Consumer stopped or starting")

        st.caption(
            f"Heartbeat age: {age:.0f}s"
            if age is not None else "No heartbeat recorded"
        )

        if not database.exists():
            st.info("Waiting for the consumer to create the prediction database.")
            return

        try:
            with sqlite3.connect(
                database.resolve().as_uri() + "?mode=ro",
                uri=True,
                timeout=5,
            ) as connection:
                total, alerts, recent = connection.execute(
                    """
                    SELECT COUNT(*), COALESCE(SUM(alert), 0),
                           COALESCE(SUM(scored_at >= ?), 0)
                    FROM predictions
                    """,
                    (now - 60,),
                ).fetchone()

                rows = pd.read_sql_query(
                    """
                    SELECT event_id, order_id, risk_score, alert,
                           emitted_at, scored_at, source_partition,
                           source_offset, batch_id
                    FROM predictions
                    ORDER BY scored_at DESC, source_offset DESC
                    LIMIT 500
                    """,
                    connection,
                )
        except sqlite3.Error as error:
            st.warning(f"Database temporarily unavailable: {error}")
            return

        columns = st.columns(4)
        for column, label, value in zip(
            columns,
            ["Unique events scored", "Review alerts", "Below threshold",
             "Scored in last 60 seconds"],
            [total, alerts, total - alerts, recent],
        ):
            column.metric(label, f"{value:,}")

        if rows.empty:
            st.info("Consumer is ready. Publish some order events.")
            return

        rows["queue_to_result_seconds"] = (
            rows["scored_at"] - rows["emitted_at"]
        ).clip(lower=0)
        rows["processed_time"] = pd.to_datetime(
            rows["scored_at"], unit="s", utc=True
        )

        history = rows.sort_values(["scored_at", "source_offset"])
        fig = px.line(
            history,
            x="processed_time",
            y="risk_score",
            markers=True,
            title="Latest actual streaming predictions",
            hover_data=["order_id", "source_offset"],
        )

        report_path = (
            root / "outputs/payment_snapshot_v1/model_metrics.json"
        )
        try:
            threshold = float(
                json.loads(
                    report_path.read_text(encoding="utf-8")
                )["threshold"]
            )
            fig.add_hline(
                y=threshold,
                line_dash="dash",
                line_color="#ce9c45",
                annotation_text="Saved alert threshold",
            )
        except (OSError, ValueError, KeyError):
            pass

        fig.update_layout(
            template="plotly_white",
            height=380,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=55, b=25),
            font_color="#526b79",
        )
        fig.update_traces(line_color="#087f83")
        st.plotly_chart(
            fig, width="stretch", config={"displaylogo": False}
        )

        st.caption(
            "Auto-refresh: 3 seconds · Chart/table: latest 500 events · "
            "Counters: all saved unique events"
        )
        st.caption(
            "Queue-to-result time includes Kafka waiting and micro-batch "
            "scheduling; it is not model-only inference latency."
        )

        alerts_only = st.checkbox(
            "Show review alerts only", key="live_alerts_only"
        )
        display = rows.loc[rows["alert"].eq(1)] if alerts_only else rows
        st.dataframe(
            display[[
                "order_id", "risk_score", "alert", "processed_time",
                "queue_to_result_seconds", "batch_id", "source_offset",
            ]],
            hide_index=True,
            width="stretch",
        )

        with st.expander("Consumer diagnostics"):
            st.json(status)
            st.caption(
                "Heartbeat and saved rows are local observations. "
                "TCP reachability alone does not verify broker health."
            )

    monitor()

    with st.expander("Publish another demonstration batch"):
        st.code(
            r".\.venv\Scripts\python.exe src\kafka_producer.py "
            "--limit 50 --interval 1",
            language="powershell",
        )

    st.caption(
        "Kafka output is at least once. The dashboard database deduplicates "
        "by event_id. Keep the checkpoint folder for restart recovery."
    )
