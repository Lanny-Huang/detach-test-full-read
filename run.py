"""
Detach/Unassign Test — Daily Refresh (v2: 6-cohort analytical redesign)
Queries Databricks, classifies engagements into acted/msg-only cohorts,
computes franchise completion + statistical comparisons, renders the viz,
and pushes to GitHub Pages.
"""
import json
import os
import subprocess
import datetime
import logging
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd
from scipy import stats as sp_stats
from jinja2 import Environment, FileSystemLoader
from databricks import sql

SCRIPT_DIR = Path(__file__).resolve().parent
DATABRICKS_HOST = "intuit-e2-739275435815-exploration-prd.cloud.databricks.com"
DATABRICKS_PATH = "/sql/1.0/warehouses/8da963fd21c39bf4"
DB_PROFILE = "intuit-e2-739275435815-exploration-prd"

MSG_DATE = datetime.date(2026, 3, 10)
DETACH_DATE = datetime.date(2026, 3, 24)
UNASSIGN_DATE = datetime.date(2026, 3, 19)

ALL_COHORTS = [
    "Holdout_Full", "Holdout_LowIntent",
    "Detach_Acted", "Detach_MsgOnly",
    "Unassign_Acted", "Unassign_MsgOnly",
]
PRIMARY_COHORTS = ["Holdout_Full", "Holdout_LowIntent", "Detach_Acted", "Unassign_Acted"]

STATE_ORDER = ["WIP_Expert", "WIP_No_Expert", "Completed_Filed", "Completed_Not_Filed"]
STATE_LABELS = {
    "WIP_Expert": "WIP w/ Expert",
    "WIP_No_Expert": "WIP No Expert",
    "Completed_Filed": "Completed — Filed",
    "Completed_Not_Filed": "Completed — Not Filed",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ── Cohort classification ─────────────────────────────────────────────────────

def load_action_ids():
    detach_ids = set(pd.read_csv(SCRIPT_DIR / "data/actual_detach.csv")["engagement_id"])
    unassign_ids = set(pd.read_csv(SCRIPT_DIR / "data/actual_unassign.csv")["engagement_id"])
    log.info(f"Loaded action IDs: {len(detach_ids)} detach, {len(unassign_ids)} unassign")
    return detach_ids, unassign_ids


def classify(engagement_id, recipe, low_intent_flag, detach_ids, unassign_ids):
    if low_intent_flag:
        return "Holdout_LowIntent"
    if recipe == "Holdout":
        return "Holdout_Full"
    if recipe == "detach messaging":
        return "Detach_Acted" if engagement_id in detach_ids else "Detach_MsgOnly"
    if recipe == "unassign messaging":
        return "Unassign_Acted" if engagement_id in unassign_ids else "Unassign_MsgOnly"
    return "Unknown"


# ── Formatting helpers ────────────────────────────────────────────────────────

def fmt_pct(rate: float) -> str:
    pct = rate * 100
    if pct == 0:
        return "0%"
    return f"{pct:.0f}%" if pct >= 1 else f"{pct:.1f}%"


def commaformat(value):
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return str(value)


def itc(treat_rate, ctrl_rate):
    if ctrl_rate == 0:
        return "∞" if treat_rate > 0 else "100"
    return str(round(treat_rate / ctrl_rate * 100))


def itc_class(treat_rate, ctrl_rate):
    if ctrl_rate == 0:
        return "itc-up" if treat_rate > 0 else "itc-neutral"
    ratio = treat_rate / ctrl_rate * 100
    if ratio > 103:
        return "itc-up"
    if ratio < 97:
        return "itc-down"
    return "itc-neutral"


def stat_test(a_yes, a_no, b_yes, b_no):
    a_yes, a_no, b_yes, b_no = max(a_yes, 0), max(a_no, 0), max(b_yes, 0), max(b_no, 0)
    if a_yes + a_no == 0 or b_yes + b_no == 0:
        return 1.0, "N/A", False
    table = [[a_yes, a_no], [b_yes, b_no]]
    if min(a_yes, a_no, b_yes, b_no) < 5:
        _, p = sp_stats.fisher_exact(table)
        test_name = "Fisher"
    else:
        _, p, _, _ = sp_stats.chi2_contingency(table, correction=False)
        test_name = "Chi²"
    return p, test_name, p < 0.05


def fmt_p(p, sig):
    if p < 0.001:
        return "< 0.001 ***"
    star = ""
    if p < 0.01:
        star = " **"
    elif p < 0.05:
        star = " *"
    return f"{p:.3f}{star}"


def fmt_p_short(p, sig):
    if p < 0.001:
        return "< 0.001 ***"
    if sig:
        return f"{p:.3f} {'**' if p < 0.01 else '*'}"
    return f"{p:.2f}"


def per1k(count, size):
    if size == 0:
        return 0
    return round(count * 1000 / size, 1)


def per1k_str(count, size):
    return f"{per1k(count, size)}/1K"


# ── Databricks ────────────────────────────────────────────────────────────────

def get_databricks_token() -> str:
    env_token = os.environ.get("DATABRICKS_TOKEN")
    if env_token:
        return env_token
    result = subprocess.run(
        ["/opt/homebrew/bin/databricks", "auth", "token", "--profile", DB_PROFILE],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)["access_token"]


def load_queries() -> dict[str, str]:
    text = (SCRIPT_DIR / "queries.sql").read_text()
    queries = {}
    current_name = None
    current_lines = []
    for line in text.splitlines():
        m = re.match(r"^--\s*@query:\s*(\S+)", line)
        if m:
            if current_name:
                queries[current_name] = "\n".join(current_lines).strip()
            current_name = m.group(1)
            current_lines = []
        else:
            current_lines.append(line)
    if current_name:
        queries[current_name] = "\n".join(current_lines).strip()
    return queries


def run_queries(token: str) -> dict[str, pd.DataFrame]:
    queries = load_queries()
    results = {}
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_PATH,
        access_token=token,
    ) as conn:
        for name, query in queries.items():
            log.info(f"Running query: {name}")
            with conn.cursor() as cur:
                cur.execute(query)
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
            results[name] = pd.DataFrame(rows, columns=cols)
            log.info(f"  -> {len(results[name])} rows")
    return results


# ── Context builder ───────────────────────────────────────────────────────────

def build_context(data: dict[str, pd.DataFrame]) -> dict:
    today = datetime.date.today()
    ctx = {}
    detach_ids, unassign_ids = load_action_ids()

    ctx["data_date"] = today.strftime("%-m/%-d")
    ctx["data_date_short"] = today.strftime("%-m/%-d")
    ctx["refresh_ts"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    ctx["days_since_detach"] = (today - DETACH_DATE).days
    ctx["days_since_unassign"] = (today - UNASSIGN_DATE).days

    # ── Classify engagements into 6 cohorts using engagement_detail ────────
    df_detail = data["engagement_detail"].copy()
    df_detail["cohort"] = df_detail.apply(
        lambda r: classify(
            r["engagement_id"], r["recipe"],
            int(r["low_intent_flag"]) == 1,
            detach_ids, unassign_ids,
        ), axis=1,
    )

    # ── Cohort sizes ──────────────────────────────────────────────────────
    sizes = df_detail.groupby("cohort")["engagement_id"].nunique().to_dict()
    for c in ALL_COHORTS:
        sizes.setdefault(c, 0)
    ctx["sizes"] = sizes
    ctx["sizes_json"] = json.dumps({c: sizes.get(c, 0) for c in ALL_COHORTS})
    log.info(f"Cohort sizes: {sizes}")

    # ── Engagement states ─────────────────────────────────────────────────
    def engagement_state(row):
        status = row.get("current_engagement_status", "")
        filed = int(row.get("fs_filed", 0)) == 1
        expert = int(row.get("expert_assigned_flag", 0)) == 1
        if status == "COMPLETED" and filed:
            return "Completed_Filed"
        if status == "COMPLETED" and not filed:
            return "Completed_Not_Filed"
        if expert:
            return "WIP_Expert"
        return "WIP_No_Expert"

    df_detail["state"] = df_detail.apply(engagement_state, axis=1)

    states_counts = defaultdict(lambda: defaultdict(int))
    for _, row in df_detail.iterrows():
        states_counts[row["cohort"]][row["state"]] += 1
    ctx["states"] = {c: dict(v) for c, v in states_counts.items()}

    state_pcts = {}
    for c in ALL_COHORTS:
        total = sizes.get(c, 1) or 1
        state_pcts[c] = {}
        for s in STATE_ORDER:
            state_pcts[c][s] = round(states_counts[c].get(s, 0) / total * 100)
    ctx["state_pcts"] = state_pcts

    state_data_for_chart = {}
    for c in ALL_COHORTS:
        total = sizes.get(c, 1) or 1
        state_data_for_chart[c] = [
            round(states_counts[c].get(s, 0) / total * 100, 1) for s in STATE_ORDER
        ]
    ctx["state_data_json"] = json.dumps(state_data_for_chart)

    state_rows = []
    for s in STATE_ORDER:
        row_data = {"label": STATE_LABELS[s]}
        for c, key in [
            ("Holdout_Full", "hf_val"), ("Holdout_LowIntent", "hli_val"),
            ("Detach_Acted", "da_val"), ("Detach_MsgOnly", "dmo_val"),
            ("Unassign_Acted", "ua_val"), ("Unassign_MsgOnly", "umo_val"),
        ]:
            cnt = states_counts[c].get(s, 0)
            total = sizes.get(c, 1) or 1
            pct = fmt_pct(cnt / total)
            row_data[key] = f"{cnt:,} ({pct})"
        state_rows.append(row_data)
    ctx["state_rows"] = state_rows

    # ── WIP clearing (off-queue) ──────────────────────────────────────────
    wip = {}
    for c in ALL_COHORTS:
        total = sizes.get(c, 0)
        on_q = states_counts[c].get("WIP_Expert", 0) + states_counts[c].get("WIP_No_Expert", 0)
        off_q = max(total - on_q, 0)
        rate = off_q / total if total else 0
        wip[c] = {"total": total, "off_queue": off_q, "rate": rate, "rate_fmt": fmt_pct(rate)}

    ctrl_full = wip.get("Holdout_Full", {"off_queue": 0, "total": 1, "rate": 0})
    for treat in ["Detach_Acted", "Detach_MsgOnly", "Unassign_Acted", "Unassign_MsgOnly"]:
        t = wip.get(treat, {"off_queue": 0, "total": 1, "rate": 0})
        p, _, sig = stat_test(
            t["off_queue"], t["total"] - t["off_queue"],
            ctrl_full["off_queue"], ctrl_full["total"] - ctrl_full["off_queue"],
        )
        wip[treat]["itc"] = itc(t["rate"], ctrl_full["rate"])
        wip[treat]["pval_label"] = f"p {fmt_p_short(p, sig)}" if sig else f"p = {p:.3f} (n.s.)"
        wip[treat]["sig"] = sig
    ctx["wip"] = wip

    da_wip = wip.get("Detach_Acted", {"rate_fmt": "N/A"})
    ua_wip = wip.get("Unassign_Acted", {"rate_fmt": "N/A"})
    full_wip = wip.get("Holdout_Full", {"rate_fmt": "N/A"})
    da_sig = wip.get("Detach_Acted", {}).get("sig", False)
    ua_sig = wip.get("Unassign_Acted", {}).get("sig", False)

    if da_sig:
        ctx["wip_narrative"] = (
            f"<strong>Detach (acted) significantly outperforms on WIP clearing</strong> "
            f"({da_wip['rate_fmt']} vs {full_wip['rate_fmt']}, ITC={da_wip.get('itc','N/A')}, {da_wip.get('pval_label','')}). "
            f"This is expected since detach system-closes engagements. "
            f"Unassign (acted) WIP clearing ({ua_wip['rate_fmt']}) is "
            f"{'significantly different from' if ua_sig else 'statistically indistinguishable from'} holdout "
            f"— unassign alone doesn't move engagements off queue."
        )
    else:
        ctx["wip_narrative"] = (
            f"Detach (acted) WIP clearing ({da_wip['rate_fmt']}) and Unassign (acted) ({ua_wip['rate_fmt']}) "
            f"compared to holdout ({full_wip['rate_fmt']})."
        )

    # ── FS filings (daily) ────────────────────────────────────────────────
    filing_totals = defaultdict(int)
    comp_daily = defaultdict(lambda: defaultdict(int))
    for _, row in df_detail.iterrows():
        c = row["cohort"]
        if int(row.get("fs_filed", 0)) == 1 and row.get("days_since_msg") is not None:
            d = int(row["days_since_msg"])
            if d >= 0:
                comp_daily[c][d] += 1
                filing_totals[c] += 1

    max_day = 15
    for c in comp_daily:
        if comp_daily[c]:
            max_day = max(max_day, max(comp_daily[c].keys()))
    ctx["max_day"] = max_day
    ctx["filing_totals"] = dict(filing_totals)

    chart_cohorts = ["Detach_Acted", "Detach_MsgOnly", "Holdout_Full", "Unassign_Acted", "Unassign_MsgOnly"]
    ctx["comp_daily_json"] = json.dumps({c: dict(comp_daily.get(c, {})) for c in chart_cohorts})

    li_filing_count = filing_totals.get("Holdout_LowIntent", 0)
    ctx["li_filing_count"] = li_filing_count
    li_size = sizes.get("Holdout_LowIntent", 1) or 1
    ctx["li_cum_per1k"] = round(li_filing_count * 1000 / li_size, 2) if li_filing_count > 0 else 0

    # ── Filing windows (computed from daily data) ─────────────────────────
    window_defs = [("0-3", 0, 3), ("0-5", 0, 5), ("0-7", 0, 7), ("0-10", 0, 10), ("all", 0, 9999)]
    window_labels = [("0-3", "0–3 days"), ("0-5", "0–5 days"), ("0-7", "0–7 days"), ("0-10", "0–10 days"), ("all", "All post-msg")]

    def window_count(cohort, lo, hi):
        return sum(v for d, v in comp_daily.get(cohort, {}).items() if lo <= d <= hi)

    def build_window_rows(ctrl_cohort, ctrl_size):
        rows = []
        for key, label in window_labels:
            lo, hi = next((lo, hi) for k, lo, hi in window_defs if k == key)
            ctrl_n = window_count(ctrl_cohort, lo, hi)
            da_n = window_count("Detach_Acted", lo, hi)
            ua_n = window_count("Unassign_Acted", lo, hi)
            da_size = sizes.get("Detach_Acted", 1) or 1
            ua_size = sizes.get("Unassign_Acted", 1) or 1
            ctrl_rate = ctrl_n / ctrl_size if ctrl_size else 0
            da_rate = da_n / da_size
            ua_rate = ua_n / ua_size

            da_p, _, da_sig = stat_test(da_n, da_size - da_n, ctrl_n, ctrl_size - ctrl_n)
            ua_p, _, ua_sig = stat_test(ua_n, ua_size - ua_n, ctrl_n, ctrl_size - ctrl_n)

            rows.append({
                "label": label,
                "ctrl_rate": fmt_pct(ctrl_rate),
                "da_rate": fmt_pct(da_rate),
                "da_itc": itc(da_rate, ctrl_rate), "da_itc_class": itc_class(da_rate, ctrl_rate),
                "da_pval": fmt_p_short(da_p, da_sig), "da_sig_class": "sig" if da_sig else "not-sig",
                "ua_rate": fmt_pct(ua_rate),
                "ua_itc": itc(ua_rate, ctrl_rate), "ua_itc_class": itc_class(ua_rate, ctrl_rate),
                "ua_pval": fmt_p_short(ua_p, ua_sig), "ua_sig_class": "sig" if ua_sig else "not-sig",
            })
        return rows

    ctx["windows_full"] = build_window_rows("Holdout_Full", sizes.get("Holdout_Full", 1))
    ctx["windows_li"] = build_window_rows("Holdout_LowIntent", sizes.get("Holdout_LowIntent", 1))

    # ── Franchise completion (auth-level, from core) ──────────────────────
    df_fc = data["franchise_completion"].copy()
    df_fc["cohort"] = df_fc.apply(
        lambda r: classify(
            r["engagement_id"], r["recipe"],
            int(r["low_intent_flag"]) == 1,
            detach_ids, unassign_ids,
        ), axis=1,
    )

    fc_stats = {}
    for c in ALL_COHORTS:
        sub = df_fc[df_fc["cohort"] == c]
        auths = sub["auth_id"].nunique()
        fs_comp = sub[sub["fs_completed"].astype(int) == 1]["auth_id"].nunique()
        fran_comp = sub[sub["franchise_completed"].astype(int) == 1]["auth_id"].nunique()
        fc_stats[c] = {
            "auths": auths,
            "fs_completers": fs_comp,
            "fs_rate": fs_comp / auths if auths else 0,
            "franchise_completers": fran_comp,
            "franchise_rate": fran_comp / auths if auths else 0,
        }
    ctx["fc_stats"] = fc_stats

    def build_fc_rows(ctrl_cohort):
        ctrl = fc_stats[ctrl_cohort]
        rows = []
        for treat_c, label in [("Detach_Acted", "Detach (Acted)"), ("Unassign_Acted", "Unassign (Acted)")]:
            t = fc_stats[treat_c]
            # FS completion
            fs_p, _, fs_sig = stat_test(
                t["fs_completers"], t["auths"] - t["fs_completers"],
                ctrl["fs_completers"], ctrl["auths"] - ctrl["fs_completers"],
            )
            # Franchise completion
            fr_p, _, fr_sig = stat_test(
                t["franchise_completers"], t["auths"] - t["franchise_completers"],
                ctrl["franchise_completers"], ctrl["auths"] - ctrl["franchise_completers"],
            )
            rows.append({
                "label": label,
                "auths": f"{t['auths']:,}",
                "fs_rate": fmt_pct(t["fs_rate"]),
                "fs_count": f"{t['fs_completers']:,}",
                "fs_itc": itc(t["fs_rate"], ctrl["fs_rate"]),
                "fs_itc_class": itc_class(t["fs_rate"], ctrl["fs_rate"]),
                "fs_pval": fmt_p(fs_p, fs_sig),
                "fs_sig_class": "sig" if fs_sig else "not-sig",
                "fr_rate": fmt_pct(t["franchise_rate"]),
                "fr_count": f"{t['franchise_completers']:,}",
                "fr_itc": itc(t["franchise_rate"], ctrl["franchise_rate"]),
                "fr_itc_class": itc_class(t["franchise_rate"], ctrl["franchise_rate"]),
                "fr_pval": fmt_p(fr_p, fr_sig),
                "fr_sig_class": "sig" if fr_sig else "not-sig",
            })
        return rows

    ctx["fc_rows_full"] = build_fc_rows("Holdout_Full")
    ctx["fc_rows_li"] = build_fc_rows("Holdout_LowIntent")
    ctx["fc_ctrl_full"] = {
        "fs_rate": fmt_pct(fc_stats["Holdout_Full"]["fs_rate"]),
        "fs_count": f"{fc_stats['Holdout_Full']['fs_completers']:,}",
        "fr_rate": fmt_pct(fc_stats["Holdout_Full"]["franchise_rate"]),
        "fr_count": f"{fc_stats['Holdout_Full']['franchise_completers']:,}",
        "auths": f"{fc_stats['Holdout_Full']['auths']:,}",
    }
    ctx["fc_ctrl_li"] = {
        "fs_rate": fmt_pct(fc_stats["Holdout_LowIntent"]["fs_rate"]),
        "fs_count": f"{fc_stats['Holdout_LowIntent']['fs_completers']:,}",
        "fr_rate": fmt_pct(fc_stats["Holdout_LowIntent"]["franchise_rate"]),
        "fr_count": f"{fc_stats['Holdout_LowIntent']['franchise_completers']:,}",
        "auths": f"{fc_stats['Holdout_LowIntent']['auths']:,}",
    }

    # ── Messaging impact (msg-only vs holdout) ────────────────────────────
    msg_impact = {}
    ctrl = fc_stats["Holdout_Full"]
    for c, label in [("Detach_MsgOnly", "Detach Msg Only"), ("Unassign_MsgOnly", "Unassign Msg Only")]:
        t = fc_stats[c]
        fs_p, _, fs_sig = stat_test(
            t["fs_completers"], t["auths"] - t["fs_completers"],
            ctrl["fs_completers"], ctrl["auths"] - ctrl["fs_completers"],
        )
        fr_p, _, fr_sig = stat_test(
            t["franchise_completers"], t["auths"] - t["franchise_completers"],
            ctrl["franchise_completers"], ctrl["auths"] - ctrl["franchise_completers"],
        )
        msg_impact[c] = {
            "label": label, "auths": f"{t['auths']:,}",
            "fs_rate": fmt_pct(t["fs_rate"]), "fs_pval": fmt_p_short(fs_p, fs_sig),
            "fs_sig_class": "sig" if fs_sig else "not-sig",
            "fr_rate": fmt_pct(t["franchise_rate"]), "fr_pval": fmt_p_short(fr_p, fr_sig),
            "fr_sig_class": "sig" if fr_sig else "not-sig",
        }
    ctx["msg_impact"] = msg_impact

    any_msg_sig = any(
        msg_impact[c]["fs_sig_class"] == "sig" or msg_impact[c].get("fr_sig_class") == "sig"
        for c in msg_impact
    )
    if any_msg_sig:
        ctx["msg_impact_narrative"] = (
            "Messaging alone shows a statistically significant effect on completion rates — "
            "even without the detach/unassign action, the message itself may have nudged some customers."
        )
    else:
        ctx["msg_impact_narrative"] = (
            "Messaging alone shows no significant difference from holdout on either FS or franchise completion. "
            "The message by itself did not measurably change customer behavior."
        )

    # ── Milestones ────────────────────────────────────────────────────────
    ms_counts = defaultdict(lambda: defaultdict(int))
    all_milestones = set()
    for _, row in df_detail.iterrows():
        c = row["cohort"]
        m = row.get("main_funnel_milestone") or "Unknown"
        ms_counts[c][m] += 1
        all_milestones.add(m)

    milestone_order = [
        "Welcome Call", "Gathering Info", "Engagement Created",
        "Tax Prep Start", "Client Review", "Evaluation", "Filing", "Post File",
    ]
    for m in sorted(all_milestones):
        if m not in milestone_order and m != "Unknown":
            milestone_order.append(m)
    ctx["milestones_list_json"] = json.dumps(milestone_order)
    ctx["ms_counts_json"] = json.dumps({c: dict(ms_counts.get(c, {})) for c in ALL_COHORTS})

    # ── DIWM ──────────────────────────────────────────────────────────────
    df_diwm = data["diwm"].copy()
    df_diwm["cohort"] = df_diwm.apply(
        lambda r: classify(
            r["engagement_id"], r["recipe"],
            int(r["low_intent_flag"]) == 1,
            detach_ids, unassign_ids,
        ), axis=1,
    )

    diwm = {}
    for c in ALL_COHORTS:
        sub = df_diwm[df_diwm["cohort"] == c]
        diwm[c] = {
            "ever_started": sub[sub["diwm_started"].astype(int) == 1]["auth_id"].nunique(),
            "started_post_msg": sub[sub["started_post_msg"].astype(int) == 1]["auth_id"].nunique(),
            "completed": sub[sub["diwm_completed"].astype(int) == 1]["auth_id"].nunique(),
            "completed_post_msg": sub[sub["completed_post_msg"].astype(int) == 1]["auth_id"].nunique(),
            "rev_post_msg": float(sub["rev_post_msg"].astype(float).sum()),
        }

    diwm_labels = [
        ("DIWM/DIY ever started (same auth)", "ever_started", True),
        ("Started after message (post 3/10)", "started_post_msg", False),
        ("DIWM/DIY completed", "completed", True),
        ("Completed after message", "completed_post_msg", False),
        ("Revenue from post-msg DIWM", "rev_post_msg", False),
    ]
    diwm_rows = []
    for label, key, show_pct in diwm_labels:
        row_data = {"label": label}
        for c, col_key in [
            ("Holdout_Full", "hf_val"), ("Holdout_LowIntent", "hli_val"),
            ("Detach_Acted", "da_val"), ("Detach_MsgOnly", "dmo_val"),
            ("Unassign_Acted", "ua_val"), ("Unassign_MsgOnly", "umo_val"),
        ]:
            val = diwm.get(c, {}).get(key, 0)
            if key == "rev_post_msg":
                row_data[col_key] = f"${val:,.0f}"
            elif show_pct:
                sz = sizes.get(c, 1) or 1
                pct = fmt_pct(val / sz)
                row_data[col_key] = f"{val:,} ({pct})"
            else:
                row_data[col_key] = str(int(val))
        diwm_rows.append(row_data)
    ctx["diwm_rows"] = diwm_rows

    post_msg_starts = sum(diwm.get(c, {}).get("started_post_msg", 0) for c in ALL_COHORTS)
    if post_msg_starts == 0:
        ctx["diwm_narrative"] = (
            "<strong>Zero new DIWM/DIY starts post-message across all cohorts.</strong> "
            "The detach pathway has not activated any DIWM/DIY re-engagement."
        )
    else:
        ctx["diwm_narrative"] = (
            f"<strong>{post_msg_starts} new DIWM/DIY start(s) post-message detected.</strong> "
            "Emerging signal worth monitoring."
        )

    # ── Appointments (daily, post-message) ────────────────────────────────
    appt_daily = defaultdict(lambda: defaultdict(int))
    appts_total = defaultdict(int)
    for _, row in df_detail.iterrows():
        c = row["cohort"]
        if row.get("appt_days_since_msg") is not None and not pd.isna(row["appt_days_since_msg"]):
            d = int(row["appt_days_since_msg"])
            if d >= 0:
                appt_daily[c][d] += 1
                appts_total[c] += 1
    ctx["appts_total"] = dict(appts_total)

    max_appt_day = 15
    for c in appt_daily:
        if appt_daily[c]:
            max_appt_day = max(max_appt_day, max(appt_daily[c].keys()) + 1)
    ctx["max_appt_day"] = max_appt_day
    ctx["appt_daily_json"] = json.dumps({
        c: dict(appt_daily.get(c, {}))
        for c in ["Detach_Acted", "Detach_MsgOnly", "Holdout_Full", "Unassign_Acted", "Unassign_MsgOnly"]
    })

    # ── Main metrics table ────────────────────────────────────────────────
    def build_main_metrics(ctrl_cohort, ctrl_size):
        metrics = []
        metric_defs = [
            ("FS Filing Rate", None, lambda c: filing_totals.get(c, 0)),
            ("Franchise Completion", "(auth-level, any product)", lambda c: fc_stats.get(c, {}).get("franchise_completers", 0)),
            ("WIP Clearing (off-queue)", None, lambda c: wip.get(c, {}).get("off_queue", 0)),
            ("Completed Not Filed", "(system closures)", lambda c: states_counts[c].get("Completed_Not_Filed", 0)),
            ("Appt Handled Post-Msg", None, lambda c: appts_total.get(c, 0)),
        ]
        for label, sublabel, count_fn in metric_defs:
            ctrl_n = count_fn(ctrl_cohort)
            row_items = []
            for treat in ["Detach_Acted", "Unassign_Acted"]:
                treat_n = count_fn(treat)
                treat_size = sizes.get(treat, 1) or 1
                ctrl_rate = ctrl_n / ctrl_size if ctrl_size else 0
                treat_rate = treat_n / treat_size
                p, _, sig = stat_test(treat_n, treat_size - treat_n, ctrl_n, ctrl_size - ctrl_n)
                row_items.append({
                    "rate": fmt_pct(treat_rate), "count": f"{treat_n:,}/{treat_size:,}",
                    "itc": itc(treat_rate, ctrl_rate), "itc_class": itc_class(treat_rate, ctrl_rate),
                    "pval": fmt_p(p, sig), "sig_class": "sig" if sig else "not-sig",
                })

            ctrl_rate = ctrl_n / ctrl_size if ctrl_size else 0
            metrics.append({
                "label": label, "sublabel": sublabel,
                "ctrl_rate": fmt_pct(ctrl_rate), "ctrl_count": f"{ctrl_n:,}/{ctrl_size:,}",
                "da": row_items[0], "ua": row_items[1],
            })
        return metrics

    ctx["main_metrics_full"] = build_main_metrics("Holdout_Full", sizes.get("Holdout_Full", 1))
    ctx["main_metrics_li"] = build_main_metrics("Holdout_LowIntent", sizes.get("Holdout_LowIntent", 1))

    # ── Summary tests ─────────────────────────────────────────────────────
    summary_tests = []
    test_specs = [
        ("FS Filing", "Detach_Acted", "Full", "Holdout_Full", dict(filing_totals)),
        ("FS Filing", "Unassign_Acted", "Full", "Holdout_Full", dict(filing_totals)),
        ("FS Filing", "Detach_Acted", "Low Int", "Holdout_LowIntent", dict(filing_totals)),
        ("FS Filing", "Unassign_Acted", "Low Int", "Holdout_LowIntent", dict(filing_totals)),
        ("Franchise Comp", "Detach_Acted", "Full", "Holdout_Full",
         {c: fc_stats.get(c, {}).get("franchise_completers", 0) for c in ALL_COHORTS}),
        ("Franchise Comp", "Unassign_Acted", "Full", "Holdout_Full",
         {c: fc_stats.get(c, {}).get("franchise_completers", 0) for c in ALL_COHORTS}),
        ("WIP Clearing", "Detach_Acted", "Full", "Holdout_Full",
         {c: wip.get(c, {}).get("off_queue", 0) for c in ALL_COHORTS}),
        ("WIP Clearing", "Unassign_Acted", "Full", "Holdout_Full",
         {c: wip.get(c, {}).get("off_queue", 0) for c in ALL_COHORTS}),
        ("Msg Only FS", "Detach_MsgOnly", "Full", "Holdout_Full", dict(filing_totals)),
        ("Msg Only FS", "Unassign_MsgOnly", "Full", "Holdout_Full", dict(filing_totals)),
    ]
    for metric, treatment, ctrl_label, ctrl_cohort, counts in test_specs:
        treat_n = counts.get(treatment, 0)
        ctrl_n = counts.get(ctrl_cohort, 0)
        treat_size = sizes.get(treatment, 1) or 1
        ctrl_size = sizes.get(ctrl_cohort, 1) or 1
        treat_rate = treat_n / treat_size
        ctrl_rate = ctrl_n / ctrl_size
        p, test_type, sig = stat_test(treat_n, treat_size - treat_n, ctrl_n, ctrl_size - ctrl_n)
        summary_tests.append({
            "metric": metric, "treatment": treatment.replace("_", " "), "control": ctrl_label,
            "treat_rate": fmt_pct(treat_rate), "ctrl_rate": fmt_pct(ctrl_rate),
            "itc": itc(treat_rate, ctrl_rate),
            "pval": "<0.001" if p < 0.001 else f"{p:.3f}",
            "test_type": test_type,
            "sig": sig, "sig_class": "sig" if sig else "not-sig",
            "sig_label": "***" if sig else "—",
        })
    ctx["summary_tests"] = summary_tests

    # ── Narrative / interpretation ────────────────────────────────────────
    da_fs_rate = fc_stats.get("Detach_Acted", {}).get("fs_rate", 0)
    ua_fs_rate = fc_stats.get("Unassign_Acted", {}).get("fs_rate", 0)
    hf_fs_rate = fc_stats.get("Holdout_Full", {}).get("fs_rate", 0)
    da_fr_rate = fc_stats.get("Detach_Acted", {}).get("franchise_rate", 0)
    ua_fr_rate = fc_stats.get("Unassign_Acted", {}).get("franchise_rate", 0)
    hf_fr_rate = fc_stats.get("Holdout_Full", {}).get("franchise_rate", 0)

    ctx["overall_recommendation"] = (
        f"Detach for immediate WIP clearing if that's the operational priority. "
        f"Franchise completion rates should be monitored weekly as the filing season progresses."
    )
    ctx["customer_outcome_summary"] = (
        f"Detach (acted) FS completion: {fmt_pct(da_fs_rate)}, franchise: {fmt_pct(da_fr_rate)}. "
        f"Unassign (acted) FS completion: {fmt_pct(ua_fs_rate)}, franchise: {fmt_pct(ua_fr_rate)}. "
        f"Holdout: FS {fmt_pct(hf_fs_rate)}, franchise {fmt_pct(hf_fr_rate)}."
    )

    return ctx


# ── Rendering & Deploy ────────────────────────────────────────────────────────

def render(ctx: dict):
    env = Environment(loader=FileSystemLoader(str(SCRIPT_DIR)))
    env.filters["commaformat"] = commaformat
    template = env.get_template("template.html")
    html = template.render(**ctx)
    out = SCRIPT_DIR / "index.html"
    out.write_text(html)
    log.info(f"Rendered {out} ({len(html):,} bytes)")


def git_push():
    os.chdir(SCRIPT_DIR)
    subprocess.run(["git", "add", "."], check=True)
    date_str = datetime.date.today().strftime("%Y-%m-%d")
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        capture_output=True,
    )
    if result.returncode == 0:
        log.info("No changes to commit.")
        return
    subprocess.run(
        ["git", "commit", "-m", f"Daily refresh {date_str}"],
        check=True,
    )
    subprocess.run(["git", "push", "origin", "main"], check=True)
    log.info(f"Pushed to GitHub Pages ({date_str}).")


def send_failure_email(error_msg: str):
    subject = "Detach Viz Daily Refresh Failed"
    body = (
        f"The detach viz refresh failed at "
        f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}.\n\nError:\n{error_msg}"
    )
    body_escaped = body.replace('"', "'").replace("\\", "/")
    script = f'''
    tell application "Mail"
        set newMsg to make new outgoing message with properties {{subject:"{subject}", content:"{body_escaped}", visible:false}}
        tell newMsg
            make new to recipient at end of to recipients with properties {{address:"lanny_huang@intuit.com"}}
        end tell
        send newMsg
    end tell
    '''
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if result.returncode == 0:
        log.info("Failure email sent.")
    else:
        log.warning(f"Could not send failure email: {result.stderr.strip()}")


if __name__ == "__main__":
    try:
        log.info("Starting detach viz refresh (v2 — 6-cohort)...")
        token = get_databricks_token()
        data = run_queries(token)
        ctx = build_context(data)
        render(ctx)
        git_push()
        log.info("Done.")
    except Exception as e:
        import traceback
        err = traceback.format_exc()
        log.error(f"Run failed: {e}")
        send_failure_email(err)
        raise
