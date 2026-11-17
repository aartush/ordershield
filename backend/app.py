import html
import json
import os
import sys
import time
import threading
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from live_view import render_live
from sklearn.metrics import precision_recall_curve

ROOT = Path(__file__).resolve().parent
VERSION = "payment_snapshot_v1"
OUT = ROOT / "outputs" / VERSION
NUM = [
    "purchase_hour", "purchase_weekday", "purchase_month", "is_weekend",
    "payment_total", "max_installments", "uses_credit_card",
    "uses_boleto", "uses_voucher", "uses_debit_card",
]
METHODS = ["credit_card", "boleto", "voucher", "debit_card"]
PAGES = [
    "Overview", "Workflow", "How It Works", "Samples", "Playground",
    "Analytics", "Performance", "Live Stream", "Replay Lab", "Documentation",
]
TEAL, GOLD, RED = "#087f83", "#ce9c45", "#cb6363"

st.set_page_config(page_title="OrderShield Studio", page_icon="🛡️", layout="wide")
st.markdown("""
<style>
:root{--ink:#172e3b;--muted:#748591;--line:#e3e9ed;--teal:#087f83}
.stApp{background:#f4f6f8;color:var(--ink);font-family:"Segoe UI",Arial,sans-serif}
.block-container,[data-testid="stMainBlockContainer"]{
max-width:none!important;width:100%!important;padding:2rem 3vw 3rem!important}
[data-testid="stMainBlockContainer"]>[data-testid="stVerticalBlock"]{gap:1.3rem}
[data-testid="stHorizontalBlock"]{gap:1.2rem}
[data-testid="stColumn"]{min-width:0}
[data-testid="stHeader"]{background:#f4f6f8}
h1,h2,h3{letter-spacing:-.04em;color:var(--ink)}
p,li{line-height:1.7}
[data-testid="stCaptionContainer"]{color:var(--muted)}
[data-testid="stSidebar"]{background:#112936;border-right:1px solid #24424f}
[data-testid="stSidebar"] h2,[data-testid="stSidebar"] p,
[data-testid="stSidebar"] label{color:#e1ecf1}
[data-testid="stSidebar"] [role="radiogroup"]{gap:5px}
[data-testid="stSidebar"] [role="radiogroup"]>label{
padding:10px 12px;border-radius:10px;border:1px solid transparent}
[data-testid="stSidebar"] [role="radiogroup"]>label:hover{background:#1b3c49}
[data-testid="stSidebar"] [role="radiogroup"]>label:has(input:checked){
background:#164b55;border-color:#286774}
.hero{padding:35px 40px;border-radius:24px;color:white;
background:radial-gradient(ellipse at top right,#246a70 0%,transparent 65%),
linear-gradient(120deg,#112936,#17414e);border:1px solid #315663}
.hero h1{color:white;font-size:clamp(2rem,3.4vw,3.7rem);
letter-spacing:-.055em;margin:10px 0 15px;padding:0;line-height:1.13}
.hero p{color:#bfd5de;margin:0;font-size:.98rem;max-width:none}
.eyebrow{font-size:10px;letter-spacing:2.5px;color:#8ed1ca;font-weight:700}
.tags{margin-top:23px;font-size:10px;letter-spacing:1.2px;color:#9fc6ce}
.card{height:100%;min-height:170px;background:white;border:1px solid var(--line);
border-radius:18px;padding:25px;box-shadow:0 4px 16px #17344305}
.card small{color:var(--teal);font-size:10px;letter-spacing:1.5px;font-weight:700}
.card h3{font-size:19px;margin:12px 0 9px;padding:0}
.card p{font-size:14px;color:#70828e;margin:0;overflow-wrap:anywhere}
[data-testid="stMetric"]{padding:22px;background:white;border:1px solid var(--line);
border-radius:17px;min-height:125px}
[data-testid="stMetricValue"]{font-size:clamp(1.5rem,2.2vw,2.5rem);
letter-spacing:-.045em;color:#173543}
[data-testid="stMetricLabel"]{color:#748591}
[data-testid="stPlotlyChart"]{background:white;border:1px solid var(--line);
border-radius:19px;padding:12px;overflow:hidden}
[data-testid="stDataFrame"]{border-radius:14px;border:1px solid var(--line);overflow:hidden}
[data-testid="stForm"]{background:white;border:1px solid var(--line);
border-radius:20px;padding:27px}
[data-testid="stForm"] [data-testid="stHorizontalBlock"]{gap:2rem}
[data-testid="stExpander"]{background:white;border-radius:13px;border-color:var(--line)}
[data-testid="stAlert"]{border-radius:12px}
[data-testid="stAlert"] p{font-size:13px}
[data-baseweb="select"]>div,[data-baseweb="input"]{border-radius:9px}
.stButton>button,.stDownloadButton>button,.stFormSubmitButton>button{
border-radius:9px;min-height:42px;font-weight:600;font-size:13px}
button[kind="primary"]{background:#087f83!important;border-color:#087f83!important;color:white!important}
hr{border-color:var(--line)!important}
.footer{font-size:11px;color:#84949f;border-top:1px solid #e3e9ed;
padding-top:18px;margin-top:20px}
@media(max-width:700px){
.block-container,[data-testid="stMainBlockContainer"]{padding:1rem!important}
.hero{padding:25px}.hero h1{font-size:2rem}.card{min-height:auto}
[data-testid="stHorizontalBlock"]{flex-wrap:wrap}
[data-testid="stHorizontalBlock"]>[data-testid="stColumn"]{
width:100%!important;flex:1 1 100%!important}}
</style>
""", unsafe_allow_html=True)


def hero(kicker, title, text):
    st.markdown(
        f'<div class="hero"><div class="eyebrow">{html.escape(kicker.upper())}</div>'
        f'<h1>{html.escape(title)}</h1><p>{html.escape(text)}</p>'
        '<div class="tags">APACHE SPARK / MLLIB / OLIST / RESEARCH STUDIO</div></div>',
        unsafe_allow_html=True,
    )


def cards(items):
    for col, (tag, title, body) in zip(st.columns(len(items)), items):
        with col:
            st.markdown(
                f'<div class="card"><small>{html.escape(tag.upper())}</small>'
                f'<h3>{html.escape(title)}</h3><p>{html.escape(body)}</p></div>',
                unsafe_allow_html=True,
            )


def plot(fig):
    fig.update_layout(
        template="plotly_white", height=380,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Segoe UI,Arial", color="#526b79", size=12),
        margin=dict(l=20, r=20, t=55, b=30),
        colorway=[TEAL, GOLD, RED, "#7497af"],
    )
    st.plotly_chart(fig, width="stretch", config={"displaylogo": False})


def metrics(items):
    for col, (label, value) in zip(st.columns(len(items)), items):
        col.metric(label, value)


def navigate(page):
    st.session_state["nav"] = page


def load_sample(row):
    st.session_state["sample"] = row.to_dict()
    st.session_state["revision"] = st.session_state.get("revision", 0) + 1
    st.session_state.pop("comparison", None)
    navigate("Playground")


@st.cache_data
def read_data(signature):
    raw = ROOT / "data/raw"
    orders = pd.read_csv(
        raw / "olist_orders_dataset.csv",
        parse_dates=["order_purchase_timestamp"],
    )
    customers = pd.read_csv(
        raw / "olist_customers_dataset.csv",
        usecols=["customer_id", "customer_state"],
    )
    orders = orders.merge(customers, on="customer_id", validate="many_to_one")
    features = pd.read_csv(
        ROOT / "data/processed/order_features.csv",
        parse_dates=["purchase_time"],
    )
    report = json.loads((OUT / "model_metrics.json").read_text(encoding="utf-8"))
    metadata = json.loads(
        (ROOT / "data/processed/feature_metadata.json").read_text(encoding="utf-8")
    )
    if report.get("feature_version") != VERSION or metadata.get("feature_version") != VERSION:
        raise ValueError("Feature/model version mismatch. Run the updated training scripts.")
    scores = pd.read_csv(OUT / "validation_predictions.csv")
    samples = features.loc[features.split.eq("validation")].merge(
        scores[["order_id", "risk_score"]], on="order_id", validate="one_to_one"
    )
    if len(samples) != len(scores):
        raise ValueError("Features and validation predictions do not match.")
    return orders, features, report, samples


@st.cache_resource
def resources(stamp):
    from pyspark.sql import SparkSession
    from pyspark.ml import PipelineModel

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    native = ROOT / "tools/hadoop-3.4.1"
    if os.name == "nt" and (native / "bin/hadoop.dll").exists():
        os.environ["HADOOP_HOME"] = str(native)
        os.environ["PATH"] = str(native / "bin") + os.pathsep + os.environ.get("PATH", "")
    spark = (
        SparkSession.builder.master("local[2]").appName("OrderShield-Studio")
        .config("spark.sql.shuffle.partitions", "4").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark, PipelineModel.load(str(ROOT / "models" / VERSION)), threading.Lock()


def predict(rows):
    try:
        from pyspark.sql import functions as F
        from pyspark.ml.functions import vector_to_array

        start = time.perf_counter()
        with st.spinner("Running the saved Spark pipeline…"):
            spark, model, lock = resources(STAMP)
            with lock:
                frame = None
                for index, row in enumerate(rows):
                    columns = [
                        F.lit(index).alias("position"),
                        F.lit(str(row["customer_state"])).alias("customer_state"),
                    ]
                    columns += [
                        F.lit(None if pd.isna(row.get(k)) else float(row[k]))
                        .cast("double").alias(k) for k in NUM
                    ]
                    part = spark.range(1).select(*columns)
                    frame = part if frame is None else frame.unionByName(part)
                scored = (
                    model.transform(frame)
                    .select("position", vector_to_array("probability")[1].alias("score"))
                    .orderBy("position").collect()
                )
        return [float(r.score) for r in scored], (time.perf_counter() - start) * 1000
    except Exception as error:
        st.error("Inference failed. Start through launch.ps1 and check Java/Hadoop paths.")
        with st.expander("Error details"):
            st.exception(error)
        return None


def gauge(score, title):
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=score,
        title={"text": title}, number={"valueformat": ".4f"},
        gauge={
            "axis": {"range": [0, 1]},
            "bar": {"color": RED if score >= THRESHOLD else TEAL},
            "threshold": {"value": THRESHOLD, "line": {"color": GOLD, "width": 4}},
        },
    ))
    plot(fig)
    st.caption("REVIEW ALERT" if score >= THRESHOLD else "BELOW ALERT THRESHOLD")


def scenario(name, seed):
    key = f"{name}_{st.session_state.get('revision', 0)}"
    st.subheader(f"Scenario {name}")
    state = str(seed.get("customer_state", "SP"))
    options = sorted(set(STATES + [state]))
    state = st.selectbox("Customer state", options, index=options.index(state), key=key+"s")
    day = st.date_input(
        "Purchase date", pd.Timestamp(seed.get("purchase_time", "2018-06-15")).date(),
        key=key+"d",
    )
    hour = st.slider("Purchase hour", 0, 23, int(seed.get("purchase_hour", 12)), key=key+"h")
    amount = seed.get("payment_total", 150.0)
    installments = seed.get("max_installments", 1)
    amount = st.number_input(
        "Payment total · BRL", min_value=0.0,
        value=150.0 if pd.isna(amount) else float(amount), key=key+"a",
    )
    installments = st.number_input(
        "Maximum installments", min_value=0, max_value=100,
        value=1 if pd.isna(installments) else int(installments), key=key+"i",
    )
    methods = st.multiselect(
        "Payment methods", METHODS,
        default=[m for m in METHODS if seed.get("uses_"+m, 0) == 1],
        key=key+"m",
    )
    weekday = (day.weekday() + 1) % 7 + 1
    return {
        "customer_state": state, "purchase_hour": hour,
        "purchase_weekday": weekday, "purchase_month": day.month,
        "is_weekend": int(weekday in [1, 7]), "payment_total": amount,
        "max_installments": installments,
        **{"uses_"+m: int(m in methods) for m in METHODS},
    }, bool(methods), str(day)


paths = [
    ROOT / "data/raw/olist_orders_dataset.csv",
    ROOT / "data/raw/olist_customers_dataset.csv",
    ROOT / "data/processed/order_features.csv",
    ROOT / "data/processed/feature_metadata.json",
    OUT / "model_metrics.json", OUT / "validation_predictions.csv",
    ROOT / "models" / VERSION / "metadata",
]
missing = [str(p.relative_to(ROOT)) for p in paths if not p.exists()]
if missing:
    hero("Setup", "Your studio is almost ready.", "Place app.py in the existing OrderShield project root.")
    st.error("Required files are missing.")
    st.code("\n".join(missing))
    st.stop()

try:
    orders, features, report, samples = read_data(
        tuple((str(p), p.stat().st_mtime_ns) for p in paths)
    )
except Exception as error:
    st.error(str(error))
    st.stop()

THRESHOLD = float(report["threshold"])
MODEL = report["selected_model"]
STAMP = paths[-1].stat().st_mtime_ns
STATES = sorted(orders.customer_state.dropna().unique().tolist())
samples["alert"] = samples.risk_score.ge(THRESHOLD)
samples["case"] = np.select(
    [
        samples.label.eq(1) & samples.alert,
        samples.label.eq(1) & ~samples.alert,
        samples.label.eq(0) & samples.alert,
    ],
    ["Detected risk", "Missed risk", "False alert"],
    default="Correctly below threshold",
)

with st.sidebar:
    st.markdown("## 🛡️ OrderShield")
    st.caption("E-COMMERCE RESEARCH STUDIO")
    page = st.radio("Navigation", PAGES, key="nav", label_visibility="collapsed")
    st.divider()
    presentation = st.toggle("Presentation mode")
    st.caption("● BATCH PIPELINE READY")
    st.caption("● SAVED MODEL AVAILABLE")
    st.caption("○ KAFKA INTEGRATION PENDING")
    st.caption("BRAZIL / 2016–2018 / BRL")


if page == "Overview":
    hero(
        "Research / engineering / exploration", "Understand risk. Explore the evidence.",
        "An interactive journey from historical e-commerce records to distributed "
        "processing, machine learning and explainable review decisions.",
    )
    metrics([
        ("Historical orders", f"{len(orders):,}"),
        ("Resolved cohort", f"{len(features):,}"),
        ("Risk cases in cohort", f"{int(features.label.sum()):,}"),
        ("Saved model", MODEL.replace("_", " ").title()),
    ])
    cards([
        ("01 / The problem", "Operational uncertainty",
         "Canceled and unavailable orders create fulfillment uncertainty. "
         "The research question is whether available order attributes contain useful early signals."),
        ("02 / The approach", "A reproducible pipeline",
         "Spark joins relational tables, builds order features and trains MLlib pipelines. "
         "Chronological validation selects the model and its alert threshold."),
        ("03 / The evidence", "Show the entire story",
         "Inspect actual predictions, missed cases and false alerts. "
         "Compare scenarios and explore how thresholds change review workload."),
    ])
    st.button("Explore the workflow →", type="primary", on_click=navigate, args=("Workflow",))
    st.info(
        "This is cancellation/unavailability research, not verified RTO prediction. "
        "Engineering stages work; current predictive performance remains limited."
    )


elif page == "Workflow":
    hero("Architecture / interactive diagram", "From raw records to a review decision.",
         "Select a stage to inspect its role, inputs and outputs. The diagram reflects the implemented pipeline.")
    stages = ["CSV Sources", "Spark ETL", "Features", "MLlib Models", "Validation", "Saved Inference"]
    selected = st.select_slider("Inspect a processing stage", stages)
    positions = [(0, 1), (1, 1), (2, 1), (2, 0), (1, 0), (0, 0)]
    fig = go.Figure()
    for i, (label, (x, y)) in enumerate(zip(stages, positions)):
        fig.add_trace(go.Scatter(
            x=[x], y=[y], mode="markers+text", text=[f"{i+1:02} · {label}"],
            textposition="bottom center", textfont=dict(size=14),
            marker=dict(size=58, color=TEAL if selected == label else "#d4e3e3"),
            hovertemplate=label+"<extra></extra>", showlegend=False,
        ))
        if i:
            a, b = positions[i-1]
            fig.add_annotation(
                x=x, y=y, ax=a, ay=b, xref="x", yref="y", axref="x", ayref="y",
                text="", arrowhead=3, arrowwidth=2, arrowcolor="#97b4bc",
                standoff=35, startstandoff=35,
            )
    fig.update_xaxes(visible=False, range=[-.45, 2.45])
    fig.update_yaxes(visible=False, range=[-.5, 1.4])
    plot(fig)
    details = {
        "CSV Sources": (
            "Olist relational tables",
            "Inspect schemas, row counts, missing values and unique keys. "
            "The model currently uses orders, customers and payments.",
            "Validated source tables",
        ),
        "Spark ETL": (
            "Orders + customers + payments",
            "Aggregate payments before joining. Left joins retain resolved orders; "
            "row-count checks prevent accidental multiplication.",
            "One row per resolved order",
        ),
        "Features": (
            "Location, purchase timing and payment snapshot",
            "Create temporal and payment indicators. Exclude IDs, actual delivery timestamps, "
            "reviews and final status from predictors.",
            "Feature CSV + version metadata",
        ),
        "MLlib Models": (
            "Training period before April 2018",
            "Fit median imputation and state encoding on training data. "
            "Train class-weighted Logistic Regression and Random Forest.",
            "Two fitted Spark pipelines",
        ),
        "Validation": (
            "April–June 2018",
            "Select by average precision, then choose the threshold maximizing validation F1. "
            "Report precision, recall, false alerts and missed risks.",
            "Selected model + frozen threshold",
        ),
        "Saved Inference": (
            "New scenario or historical replay input",
            "Reload preprocessing and classifier together. Compute an uncalibrated score "
            "and compare it with the saved threshold.",
            "Review alert or below threshold",
        ),
    }
    incoming, operation, outgoing = details[selected]
    cards([("Input", selected, incoming), ("Operation", "What happens", operation),
           ("Output", "What is produced", outgoing)])
    with st.expander("Kafka streaming architecture"):
        st.markdown(
            "**Kafka order events → Spark Structured Streaming → same feature contract "
            "→ saved MLlib pipeline → prediction output.**\n\n"
            "Broker, producer and Spark consumer are connected. Live Stream displays saved predictions. Restart recovery is verified separately."
        )


elif page == "How It Works":
    hero("Guided explanation", "Follow one real order, step by step.",
         "Explore the exact features and stored model score before revealing the historical label.")
    index = st.slider("Validation example", 1, len(samples), 1)
    row = samples.sort_values("order_id").iloc[index-1]
    step = st.radio("Order journey", ["Inputs", "Features", "Model score", "Decision", "Outcome"], horizontal=True)
    st.caption(f"ORDER / {row.order_id}")
    if step == "Inputs":
        cards([
            ("Location", str(row.customer_state), "Customer state used by the fitted state encoder."),
            ("Purchase", str(row.purchase_time), "Hour, weekday, month and weekend are derived from this timestamp."),
            ("Payment", f"BRL {row.payment_total:,.2f}", "Historical payment snapshot aggregated to the order level."),
        ])
        st.caption("Payment availability at scoring time is assumed; payment event timestamps are not present.")
    elif step == "Features":
        st.dataframe(pd.DataFrame({
            "Feature": ["customer_state"] + NUM,
            "Value": [str(row[k]) for k in ["customer_state"] + NUM],
        }), hide_index=True, width="stretch")
        st.write("Missing numeric values use training medians. State encoding uses the fitted training vocabulary.")
    elif step == "Model score":
        gauge(float(row.risk_score), "Stored validation score")
        st.caption("The score is from the trained model, not a hand-written risk rule or calibrated probability.")
    elif step == "Decision":
        st.code(
            f"risk_score = {row.risk_score:.6f}\n"
            f"threshold  = {THRESHOLD:.6f}\n"
            f"alert      = risk_score >= threshold\n"
            f"result     = {bool(row.alert)}"
        )
    elif st.checkbox("Reveal actual historical outcome"):
        st.metric("Actual class", "Canceled / unavailable" if row.label else "Delivered")
        st.write(f"Decision category: **{row['case']}**")
    st.button("Try this order in Playground →", on_click=load_sample, args=(row,))


elif page == "Samples":
    hero("Evidence / sample explorer", "Inspect the hits and the misses.",
         "Actual validation examples show successful detections, missed risks and unnecessary alerts.")
    category = st.selectbox("Decision category", [
        "Detected risk", "Missed risk", "False alert", "Correctly below threshold",
    ])
    pool = samples.loc[samples["case"].eq(category)].sort_values("risk_score", ascending=False)
    if pool.empty:
        st.info("No examples exist in this category.")
    else:
        order_id = st.selectbox("Select an order", pool.order_id.tolist())
        row = pool.loc[pool.order_id.eq(order_id)].iloc[0]
        left, right = st.columns(2)
        with left:
            gauge(float(row.risk_score), "Saved model score")
        with right:
            st.subheader(category)
            st.write(f"**State:** {row.customer_state}")
            st.write(f"**Purchase:** {row.purchase_time}")
            st.write(f"**Payment:** BRL {row.payment_total:,.2f}")
            st.write(f"**Actual class:** {'Canceled / unavailable' if row.label else 'Delivered'}")
            st.caption(f"{len(pool):,} validation examples belong to this category.")
            st.button("Load into Playground →", type="primary", on_click=load_sample, args=(row,))
        st.dataframe(
            pool[["order_id", "customer_state", "payment_total", "risk_score"]].head(100),
            hide_index=True, width="stretch",
        )


elif page == "Playground":
    hero("Experiment / scenario comparison", "Change an input. Compare the response.",
         "Two scenarios pass through the same saved Spark pipeline. Scores are calculated when you submit.")
    seed = st.session_state.get("sample", {
        "customer_state": "SP", "purchase_time": "2018-06-15",
        "purchase_hour": 12, "payment_total": 150.0,
        "max_installments": 1, "uses_credit_card": 1,
    })
    st.caption("Change one variable in B for a clearer comparison. Associations are not causal effects.")
    with st.form("compare"):
        left, right = st.columns(2)
        with left:
            a, valid_a, day_a = scenario("A", seed)
        with right:
            b, valid_b, day_b = scenario("B", seed)
        submitted = st.form_submit_button("Run Spark comparison →", type="primary")
    if submitted:
        st.session_state.pop("comparison", None)
        if not valid_a or not valid_b:
            st.error("Select at least one payment method in both scenarios.")
        else:
            result = predict([a, b])
            if result:
                st.session_state["comparison"] = dict(
                    scores=result[0], milliseconds=result[1],
                    A=a, B=b, date_A=day_a, date_B=day_b,
                )
    result = st.session_state.get("comparison")
    if result:
        st.caption("Last submitted comparison. Submit again after editing inputs.")
        for col, title, score in zip(st.columns(2), ["Scenario A", "Scenario B"], result["scores"]):
            with col:
                gauge(score, title)
        metrics([
            ("Score difference · B minus A", f"{result['scores'][1]-result['scores'][0]:+.4f}"),
            ("Alert threshold", f"{THRESHOLD:.6f}"),
            ("Spark call incl. initialization", f"{result['milliseconds']:,.0f} ms"),
        ])
        if any(not ("2016-01-01" <= result[k] <= "2018-12-31") for k in ["date_A", "date_B"]):
            st.warning("An input date is outside the dataset years. Current-world performance is unverified.")
        with st.expander("Inspect the exact feature inputs"):
            st.dataframe(pd.DataFrame([result["A"], result["B"]], index=["A", "B"]).T, width="stretch")
        st.download_button(
            "Download comparison", json.dumps(result, indent=2),
            "ordershield_comparison.json", "application/json",
        )
    st.caption("These class-weighted scores are not cancellation percentages.")


elif page == "Analytics":
    hero("Explore / historical data", "See the operational picture.",
         "Filter original orders by customer state and inspect volume, status mix and historical risk share.")
    states = st.multiselect("Customer states · empty means all", STATES)
    view = orders.loc[orders.customer_state.isin(states)] if states else orders
    risk = view.order_status.isin(["canceled", "unavailable"])
    metrics([
        ("Orders", f"{len(view):,}"),
        ("Delivered", f"{view.order_status.eq('delivered').sum():,}"),
        ("Canceled / unavailable", f"{risk.sum():,}"),
        ("Risk share of all filtered orders", f"{risk.mean():.2%}"),
    ])
    left, right = st.columns(2)
    with left:
        counts = view.order_status.value_counts().rename_axis("status").reset_index(name="orders")
        plot(px.bar(counts, x="status", y="orders", title="Order status distribution"))
    with right:
        monthly = view.groupby(view.order_purchase_timestamp.dt.to_period("M")).size().reset_index(name="orders")
        monthly["order_purchase_timestamp"] = monthly.order_purchase_timestamp.astype(str)
        plot(px.line(monthly, x="order_purchase_timestamp", y="orders", markers=True, title="Monthly order volume"))
    by_state = view.assign(risky=risk).groupby("customer_state").agg(
        orders=("order_id", "size"), risky=("risky", "sum")
    ).reset_index()
    by_state["risk_share"] = by_state.risky / by_state.orders
    plot(px.bar(
        by_state.sort_values("orders", ascending=False), x="customer_state", y="orders",
        color="risk_share", color_continuous_scale="Teal",
        title="State volume · color represents canceled/unavailable share",
    ))
    st.caption("Analytics includes unresolved statuses; the model cohort excludes them.")
    st.download_button(
        "Download filtered orders", view.to_csv(index=False).encode("utf-8"),
        "ordershield_orders.csv", "text/csv",
    )


elif page == "Performance":
    hero("Evaluate / threshold laboratory", "A useful model must show its mistakes.",
         "Inspect validation results and explore the trade-off between catching risky orders and generating review work.")
    st.info("Validation selected the model and threshold. These are not independent final-test estimates.")
    comparison = pd.DataFrame(report["validation_results"]).T
    st.dataframe(
        comparison[["average_precision", "roc_auc", "precision", "recall", "f1", "alert_rate"]]
        .astype(float).round(4), width="stretch",
    )
    threshold = st.slider(
        "Experimental threshold · saved settings stay unchanged",
        0.0, 1.0, THRESHOLD, .001, format="%.3f",
    )
    actual, predicted = samples.label.eq(1), samples.risk_score.ge(threshold)
    tp, fp = int((actual & predicted).sum()), int((~actual & predicted).sum())
    fn, tn = int((actual & ~predicted).sum()), int((~actual & ~predicted).sum())
    precision, recall = tp/max(tp+fp, 1), tp/max(tp+fn, 1)
    metrics([
        ("Precision", f"{precision:.2%}"), ("Recall", f"{recall:.2%}"),
        ("Review alerts", f"{tp+fp:,}"), ("Missed risky orders", f"{fn:,}"),
    ])
    left, right = st.columns(2)
    with left:
        matrix = pd.DataFrame(
            [[tn, fp], [fn, tp]], index=["Actual delivered", "Actual risky"],
            columns=["Below threshold", "Review alert"],
        )
        plot(px.imshow(matrix, text_auto=True, color_continuous_scale="Teal", title="Confusion matrix"))
    with right:
        p, r, _ = precision_recall_curve(samples.label, samples.risk_score)
        fig = go.Figure(go.Scatter(x=r, y=p, mode="lines", name="Model", line_color=TEAL))
        fig.add_hline(y=float(actual.mean()), line_dash="dash", line_color=GOLD)
        fig.add_trace(go.Scatter(
            x=[recall], y=[precision], mode="markers", name="Current threshold",
            marker=dict(color=RED, size=12),
        ))
        fig.update_layout(title="Precision–recall curve", xaxis_title="Recall", yaxis_title="Precision")
        plot(fig)
    st.warning("Current predictive quality is limited. Many risky orders are missed and false alerts remain.")
    st.caption("The earlier baseline test set was inspected. A pristine final evaluation requires new unseen data.")
    st.download_button(
        "Download model report", json.dumps(report, indent=2),
        "ordershield_report.json", "application/json",
    )


elif page == "Live Stream":
    render_live(ROOT)


elif page == "Replay Lab":
    hero("Demonstrate / historical replay", "Send five orders through the model.",
         "Manually replay validation inputs and watch actual saved-model inference accumulate in this session.")
    st.info("Manual replay only. This is not Kafka or Spark Structured Streaming.")
    st.session_state.setdefault("replay", [])
    st.session_state.setdefault("cursor", 0)
    left, right = st.columns([1, 3])
    if left.button("Reset session"):
        st.session_state["replay"], st.session_state["cursor"] = [], 0
    if right.button("Score next 5 orders →", type="primary"):
        queue = samples.sort_values("order_id").reset_index(drop=True)
        batch = queue.iloc[st.session_state["cursor"]:st.session_state["cursor"]+5]
        if batch.empty:
            st.info("All validation orders have been replayed.")
        else:
            result = predict(batch.to_dict("records"))
            if result:
                for row, score in zip(batch.to_dict("records"), result[0]):
                    st.session_state["replay"].append(dict(
                        sequence=len(st.session_state["replay"])+1,
                        order_id=row["order_id"], state=row["customer_state"],
                        risk_score=score, alert=score >= THRESHOLD,
                    ))
                st.session_state["cursor"] += len(batch)
                st.caption(f"Batch inference: {result[1]:,.0f} ms")
    replay = pd.DataFrame(st.session_state["replay"])
    if replay.empty:
        st.write("Ready. The first batch starts Spark, loads the saved pipeline and scores five inputs.")
    else:
        metrics([
            ("Orders actually scored", len(replay)),
            ("Review alerts", int(replay.alert.sum())),
        ])
        plot(px.line(replay, x="sequence", y="risk_score", markers=True, title="Historical replay scores"))
        st.dataframe(replay.iloc[::-1], hide_index=True, width="stretch")


elif page == "Documentation":
    hero("Reference / reproducibility", "The project, explained.",
         "A practical reference for the dataset, pipeline, evaluation choices and limitations.")
    docs = {
        "01 · Problem statement": """
The objective is to study whether location, purchase timing and payment attributes
can help prioritize orders that eventually become canceled or unavailable.

The output is a **model score and review decision**. It is not verified RTO
prediction, a guaranteed outcome or a calibrated probability.

The completed engineering components are batch validation, Spark feature building,
MLlib training, persistence/reload, interactive inference and manual replay.
""",
        "02 · Dataset and joins": """
The Olist dataset contains Brazilian marketplace orders from 2016–2018.
Available tables cover orders, customers, items, payments, products, sellers,
geolocation, reviews and category translation.

The current feature pipeline uses **orders + customers + aggregated payments**.
Payments are grouped by order before the join to avoid multiplying order rows.
Unique-key checks and final row-count checks protect the feature table.

Items are excluded: **603 of 609 unavailable orders lacked item records** in the
inspected snapshot. Using that absence could exploit the recording process.

[Original dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
""",
        "03 · Target and feature contract": """
**Positive class:** canceled or unavailable.  
**Negative class:** delivered.  
**Excluded:** shipped, invoiced, processing, created and approved.

Features include customer state; purchase hour, weekday, month and weekend;
payment total; maximum installments; and four payment-method indicators.

Missing numbers use training medians. State indexing and one-hot encoding
are fitted on training data. IDs, labels, split identifiers, actual delivery
timestamps and reviews are excluded from prediction.
""",
        "04 · Training and evaluation": """
**Training:** before April 1, 2018.  
**Validation:** April 1–June 30, 2018.  
**Test period:** July 1, 2018 onward.

Logistic Regression and Random Forest use positive-class weighting derived
only from training counts. Model selection maximizes validation average precision.
The alert threshold maximizes validation F1.

Precision describes the fraction of alerts that are actual risky orders.
Recall describes the fraction of risky orders detected. False alerts represent
unnecessary reviews; false negatives are missed risky orders.

Average precision summarizes ranking quality and is not identical to
trapezoidal PR-AUC. Accuracy alone is unsuitable for this imbalanced problem.
""",
        "05 · Inference and reproducibility": """
The saved artifact includes numeric imputation, categorical encoding,
feature assembly and the classifier. Inference reuses the same transformations.

The dashboard starts Spark lazily when scoring is requested. Analytics and
documentation do not start the JVM. The launcher configures project-local
Hadoop paths for Windows without changing system-wide variables.

After retraining, restart the dashboard to ensure the new model is loaded.
""",
        "06 · Limitations and honest interpretation": """
- Current model performance is weak; this is a research demonstration.
- Payment records lack event timestamps. Availability at scoring time is assumed.
- Outcome-resolution timestamps are absent, limiting point-in-time reconstruction.
- Validation results are used for selection and are not unbiased final estimates.
- The original test set was already inspected; a new independent holdout is needed.
- Class-weighted scores are not calibrated cancellation probabilities.
- Scenario changes indicate associations, not causal effects.
- Brazil 2016–2018 data does not establish present-day Indian COD/RTO performance.
- Spark runs locally. Multi-node scalability has not been benchmarked.
- Kafka and Structured Streaming are connected; restart recovery and final integration checks remain.
""",
        "07 · Viva discussion": """
**Why Spark?** A consistent API for distributed ETL and ML pipelines.
This demonstration uses local execution rather than claiming a tested cluster.

**Why aggregate before joining?** Payments have multiple rows per order.
A direct multi-table join could multiply records and inflate totals.

**Why a chronological split?** It approximates prediction on later orders
and avoids mixing future purchases into the training period.

**Why exclude reviews and actual delivery dates?** They occur after the intended
prediction stage and would introduce future information.

**Why are results modest?** Available features contain limited signal, the
target is rare and prevalence changes across periods.

**What comes next?** Timestamped feature data, independent evaluation,
then Kafka ingestion, Structured Streaming, checkpointing and recovery tests.
""",
    }
    for title, content in docs.items():
        with st.expander(title, expanded=title.startswith("01")):
            st.markdown(content)
    with st.expander("08 · Run commands"):
        st.code(
            "python src/validate_data.py\n"
            "python src/build_features.py\n"
            "python src/train_model.py\n"
            ".\\launch.ps1", language="powershell",
        )

if presentation:
    cues = {
        "Overview": "Explain the business question, then distinguish working engineering from predictive quality.",
        "Workflow": "Move through the diagram and explain why payments are aggregated before joins.",
        "How It Works": "Explain inputs → features → score → threshold, then reveal the outcome.",
        "Samples": "Show one detection, one missed risk and one false alert.",
        "Playground": "Change only one input in B and compare real Spark model scores.",
        "Analytics": "Discuss class imbalance and the difference between the full dataset and resolved cohort.",
        "Performance": "Move the threshold and explain review workload versus risk detection.",
        "Replay Lab": "Score five orders and clearly distinguish manual replay from Kafka streaming.",
        "Documentation": "Explain design decisions and limitations confidently.",
    }
    st.divider()
    st.info("Presentation cue · " + cues.get(page, "Show incoming predictions, explain micro-batches and distinguish model scores from probabilities."))

st.markdown(
    '<div class="footer">OrderShield Research Studio · Historical Olist data · '
    'Uncalibrated model scores · Local Apache Spark demonstration</div>',
    unsafe_allow_html=True,
)
