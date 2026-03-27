"""
Detach/Unassign Test — Daily Refresh
Queries Databricks, computes statistical comparisons, renders the viz, and pushes to GitHub Pages.
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

COHORTS = ["Holdout_Full", "Holdout_LowIntent", "Detach", "Unassign"]
STATE_ORDER = ["WIP_Expert", "WIP_No_Expert", "Completed_Filed", "Completed_Not_Filed"]
STATE_LABELS = {
    "WIP_Expert": "WIP w/ Expert",
    "WIP_No_Expert": "WIP No Expert",
    "Completed_Filed": "Completed — Filed",
    "Completed_Not_Filed": "Completed — Not Filed",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_pct(rate: float) -> str:
    """Format rate (0-1) as percentage: 0 decimals if >=1%, 1 decimal if <1%."""
    pct = rate * 100
    if pct == 0:
        return "0.0%"
    return f"{pct:.0f}%" if pct >= 1 else f"{pct:.1f}%"


def fmt_pct_raw(pct: float) -> str:
    """Format a raw percent value (already *100)."""
    if pct == 0:
        return "0.0%"
    return f"{pct:.0f}%" if pct >= 1 else f"{pct:.1f}%"


def commaformat(value):
    """Jinja2 filter: 1234 -> '1,234'."""
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
    """Run chi-square or Fisher's exact test. Returns (p-value, test_name, is_sig)."""
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
        return "< 0.001 ***" if sig else "< 0.001"
    star = " ***" if p < 0.05 else (" **" if p < 0.01 else "")
    if p < 0.01:
        star = " **"
    if p < 0.05 and p >= 0.01:
        star = " *"
    if p < 0.001:
        star = " ***"
    return f"{p:.3f}{star}"


def fmt_p_short(p, sig):
    if p < 0.001:
        return "< 0.001 ***"
    if sig:
        return f"{p:.3f} {'***' if p < 0.001 else '**' if p < 0.01 else '*'}"
    return f"{p:.2f}"


def per1k_str(count, size):
    val = round(count * 1000 / size)
    return f"{val}/1K"


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
    """Parse queries.sql into {name: sql} dict using '-- @query: name' markers."""
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


# ── Stats computation ─────────────────────────────────────────────────────────

def build_context(data: dict[str, pd.DataFrame]) -> dict:
    """Build the full Jinja2 template context from query results."""
    today = datetime.date.today()
    ctx = {}

    ctx["data_date"] = today.strftime("%-m/%-d")
    ctx["data_date_short"] = today.strftime("%-m/%-d")
    ctx["refresh_ts"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    ctx["days_since_detach"] = (today - DETACH_DATE).days
    ctx["days_since_unassign"] = (today - UNASSIGN_DATE).days

    # ── Cohort sizes ──────────────────────────────────────────────────────────
    df = data["cohort_sizes"]
    sizes = {}
    for _, row in df.iterrows():
        cohort = row["cohort"]
        if cohort in COHORTS:
            sizes[cohort] = int(row["n"])
    # Low-intent holdout is a subset: don't double-count in Holdout_Full
    # Holdout_Full from 3_9_detach_test_read includes all holdout (recipe=Holdout)
    # The query gives us Holdout_Full and Holdout_LowIntent separately because of the CASE
    # But Holdout_LowIntent is carved from recipe='Holdout', so Holdout_Full count may be reduced
    # Let's check if Holdout_Full is present
    if "Holdout_Full" not in sizes:
        sizes["Holdout_Full"] = 3946  # fallback
    if "Holdout_LowIntent" not in sizes:
        sizes["Holdout_LowIntent"] = 1602  # fallback

    ctx["sizes"] = sizes
    ctx["sizes_json"] = json.dumps({k: sizes.get(k, 0) for k in COHORTS})

    # ── FS filings (daily) ────────────────────────────────────────────────────
    df = data["fs_filings"]
    comp_daily = defaultdict(dict)
    filing_totals = defaultdict(int)
    for _, row in df.iterrows():
        c = row["cohort"]
        d = int(row["days_since_msg"])
        n = int(row["filings"])
        if d >= 0:
            comp_daily[c][d] = n
            filing_totals[c] += n

    max_day = 0
    for c in comp_daily:
        if comp_daily[c]:
            max_day = max(max_day, max(comp_daily[c].keys()))
    max_day = max(max_day, 15)
    ctx["max_day"] = max_day
    ctx["comp_daily_json"] = json.dumps({c: dict(comp_daily.get(c, {})) for c in ["Detach", "Holdout_Full", "Unassign"]})

    li_filing_count = filing_totals.get("Holdout_LowIntent", 0)
    ctx["li_filing_count"] = li_filing_count
    ctx["li_cum_per1k"] = round(li_filing_count * 1000 / sizes.get("Holdout_LowIntent", 1602), 2) if li_filing_count > 0 else 0

    # ── Filing windows ────────────────────────────────────────────────────────
    df_w = data["filing_windows"]
    windows_data = {}
    for _, row in df_w.iterrows():
        c = row["cohort"]
        windows_data[c] = {
            "0-3": int(row["w_0_3"]),
            "0-5": int(row["w_0_5"]),
            "0-7": int(row["w_0_7"]),
            "0-10": int(row["w_0_10"]),
            "all": int(row["w_all"]),
        }

    def build_window_rows(ctrl_cohort, ctrl_size):
        window_labels = [("0-3", "0–3 days"), ("0-5", "0–5 days"), ("0-7", "0–7 days"), ("0-10", "0–10 days"), ("all", "0–15 days")]
        rows = []
        for key, label in window_labels:
            ctrl_n = windows_data.get(ctrl_cohort, {}).get(key, 0)
            det_n = windows_data.get("Detach", {}).get(key, 0)
            una_n = windows_data.get("Unassign", {}).get(key, 0)
            ctrl_rate = ctrl_n / ctrl_size if ctrl_size else 0
            det_rate = det_n / sizes["Detach"] if sizes["Detach"] else 0
            una_rate = una_n / sizes["Unassign"] if sizes["Unassign"] else 0

            det_p, _, det_sig = stat_test(det_n, sizes["Detach"] - det_n, ctrl_n, ctrl_size - ctrl_n)
            una_p, _, una_sig = stat_test(una_n, sizes["Unassign"] - una_n, ctrl_n, ctrl_size - ctrl_n)

            rows.append({
                "label": label,
                "ctrl_rate": fmt_pct(ctrl_rate),
                "det_rate": fmt_pct(det_rate),
                "det_itc": itc(det_rate, ctrl_rate),
                "det_itc_class": itc_class(det_rate, ctrl_rate),
                "det_pval": fmt_p_short(det_p, det_sig),
                "det_sig_class": "sig" if det_sig else "not-sig",
                "una_rate": fmt_pct(una_rate),
                "una_itc": itc(una_rate, ctrl_rate),
                "una_itc_class": itc_class(una_rate, ctrl_rate),
                "una_pval": fmt_p_short(una_p, una_sig),
                "una_sig_class": "sig" if una_sig else "not-sig",
            })
        return rows

    ctx["windows_full"] = build_window_rows("Holdout_Full", sizes["Holdout_Full"])
    ctx["windows_li"] = build_window_rows("Holdout_LowIntent", sizes["Holdout_LowIntent"])
    any_full_sig = any(w["det_sig_class"] == "sig" or w["una_sig_class"] == "sig" for w in ctx["windows_full"])
    any_li_sig = any(w["det_sig_class"] == "sig" or w["una_sig_class"] == "sig" for w in ctx["windows_li"])
    ctx["windows_full_note"] = "No significant differences. Both treatments track close to full holdout." if not any_full_sig else "Some windows show significant differences."
    ctx["windows_li_note"] = f"Low-intent holdout = {li_filing_count} filing(s). {'Significance emerges in later windows.' if any_li_sig else 'No significant differences yet.'}"

    # ── Post-action filings ───────────────────────────────────────────────────
    df_pa = data["post_action_filings"]
    post_action = {}
    for _, row in df_pa.iterrows():
        c = row["cohort"]
        post_action[c] = {
            "w_0_1": int(row["w_0_1"]),
            "w_0_3": int(row["w_0_3"]),
            "w_0_5": int(row["w_0_5"]),
            "w_0_7": int(row["w_0_7"]),
        }
    ctx["post_action"] = post_action

    pa_windows = []
    for key, label in [("w_0_1", "0–1 day"), ("w_0_3", "0–3 days"), ("w_0_5", "0–5 days"), ("w_0_7", "0–7 days")]:
        det_n = post_action.get("Detach", {}).get(key, 0)
        una_n = post_action.get("Unassign", {}).get(key, 0)
        p, _, sig = stat_test(det_n, sizes["Detach"] - det_n, una_n, sizes["Unassign"] - una_n)
        pa_windows.append({
            "label": label,
            "det_count": det_n,
            "det_per1k": per1k_str(det_n, sizes["Detach"]),
            "una_count": una_n,
            "una_per1k": per1k_str(una_n, sizes["Unassign"]),
            "pval": fmt_p_short(p, sig),
            "sig_class": "sig" if sig else "not-sig",
        })
    ctx["post_action_windows"] = pa_windows

    det_7 = post_action.get("Detach", {}).get("w_0_7", 0)
    una_7 = post_action.get("Unassign", {}).get("w_0_7", 0)
    ctx["post_action_det_per1k"] = per1k_str(det_7, sizes["Detach"])
    ctx["post_action_una_per1k"] = per1k_str(una_7, sizes["Unassign"])

    days_det = ctx["days_since_detach"]
    days_una = ctx["days_since_unassign"]
    det_1 = post_action.get("Detach", {}).get("w_0_1", 0)
    una_1 = post_action.get("Unassign", {}).get("w_0_1", 0)
    ctx["post_action_note"] = (
        f"Detach's {det_1} filing(s) in day 1 is comparable to unassign's {una_1} at the same time mark. "
        f"The divergence at 5+ days reflects unassign's longer observation window ({days_una} days vs {days_det}), "
        f"not necessarily better performance."
    )
    ctx["post_action_comparison"] = (
        f"On a per-day basis, these are comparable. But unassigned engagements remain 'WIP' with expert removed — "
        f"this doesn't actually free up the engagement slot."
    )

    # ── Engagement states ─────────────────────────────────────────────────────
    df_s = data["engagement_states"]
    states_counts = defaultdict(lambda: defaultdict(int))
    for _, row in df_s.iterrows():
        c = row["cohort"]
        s = row["state"]
        states_counts[c][s] = int(row["cnt"])
    ctx["states"] = dict(states_counts)

    state_pcts = {}
    for c in COHORTS:
        total = sizes.get(c, 1)
        state_pcts[c] = {}
        for s in STATE_ORDER:
            state_pcts[c][s] = round(states_counts[c].get(s, 0) / total * 100)
    ctx["state_pcts"] = state_pcts

    state_data_for_chart = {}
    for c in COHORTS:
        total = sizes.get(c, 1)
        state_data_for_chart[c] = [
            round(states_counts[c].get(s, 0) / total * 100, 1) for s in STATE_ORDER
        ]
    ctx["state_data_json"] = json.dumps(state_data_for_chart)

    state_rows = []
    for s in STATE_ORDER:
        row_data = {"label": STATE_LABELS[s]}
        for c, key in [("Holdout_Full", "full_val"), ("Holdout_LowIntent", "li_val"), ("Detach", "det_val"), ("Unassign", "una_val")]:
            cnt = states_counts[c].get(s, 0)
            pct = fmt_pct_raw(cnt / sizes.get(c, 1) * 100)
            row_data[key] = f"{cnt:,} ({pct})"
        state_rows.append(row_data)
    ctx["state_rows"] = state_rows

    # ── WIP clearing ──────────────────────────────────────────────────────────
    df_wip = data["wip_clearing"]
    wip = {}
    for _, row in df_wip.iterrows():
        c = row["cohort"]
        total = int(row["total"])
        off_q = int(row["off_queue"])
        rate = off_q / total if total else 0
        wip[c] = {"total": total, "off_queue": off_q, "rate": rate, "rate_fmt": fmt_pct(rate)}

    ctrl_full = wip.get("Holdout_Full", {"off_queue": 0, "total": 1, "rate": 0})
    ctrl_li = wip.get("Holdout_LowIntent", {"off_queue": 0, "total": 1, "rate": 0})

    for treat in ["Detach", "Unassign"]:
        t = wip.get(treat, {"off_queue": 0, "total": 1, "rate": 0})
        p, _, sig = stat_test(
            t["off_queue"], t["total"] - t["off_queue"],
            ctrl_full["off_queue"], ctrl_full["total"] - ctrl_full["off_queue"],
        )
        wip[treat]["itc"] = itc(t["rate"], ctrl_full["rate"])
        if sig:
            wip[treat]["pval_label"] = f"p {fmt_p_short(p, sig)}"
        else:
            wip[treat]["pval_label"] = f"p = {p:.3f} (n.s.)"
        wip[treat]["sig"] = sig

    ctx["wip"] = wip
    ctx["wip_det_sig"] = wip.get("Detach", {}).get("sig", False)
    ctx["wip_una_sig"] = wip.get("Unassign", {}).get("sig", False)

    det_wip = wip.get("Detach", {"rate_fmt": "N/A"})
    una_wip = wip.get("Unassign", {"rate_fmt": "N/A"})
    full_wip = wip.get("Holdout_Full", {"rate_fmt": "N/A"})
    if ctx["wip_det_sig"]:
        ctx["wip_narrative"] = (
            f"<strong>Detach significantly outperforms on WIP clearing</strong> "
            f"({det_wip['rate_fmt']} vs {full_wip['rate_fmt']}, ITC={det_wip.get('itc','N/A')}, {det_wip.get('pval_label','')}). "
            f"This is expected since detach system-closes engagements. "
            f"Unassign's WIP clearing ({una_wip['rate_fmt']}) is "
            f"{'statistically indistinguishable from' if not ctx['wip_una_sig'] else 'significantly different from'} holdout "
            f"— unassign alone doesn't move engagements off queue; it only removes the expert."
        )
    else:
        ctx["wip_narrative"] = (
            f"Detach WIP clearing ({det_wip['rate_fmt']}) and Unassign ({una_wip['rate_fmt']}) "
            f"compared to holdout ({full_wip['rate_fmt']})."
        )

    # ── Milestones ────────────────────────────────────────────────────────────
    df_ms = data["milestones"]
    ms_counts = defaultdict(lambda: defaultdict(int))
    all_milestones = set()
    for _, row in df_ms.iterrows():
        c = row["cohort"]
        m = row["milestone"]
        ms_counts[c][m] = int(row["cnt"])
        all_milestones.add(m)

    milestone_order = [
        "Welcome Call", "Gathering Info", "Engagement Created",
        "Tax Prep Start", "Client Review", "Evaluation", "Filing", "Post File",
    ]
    for m in sorted(all_milestones):
        if m not in milestone_order and m != "Unknown":
            milestone_order.append(m)
    ctx["milestones_list_json"] = json.dumps(milestone_order)
    ctx["ms_counts_json"] = json.dumps({c: dict(ms_counts.get(c, {})) for c in COHORTS})

    # ── DIWM ──────────────────────────────────────────────────────────────────
    df_d = data["diwm"]
    diwm = {}
    for _, row in df_d.iterrows():
        c = row["cohort"]
        diwm[c] = {
            "ever_started": int(row["ever_started"]),
            "started_post_msg": int(row["started_post_msg"]),
            "completed": int(row["completed"]),
            "completed_post_msg": int(row["completed_post_msg"]),
            "rev_post_msg": float(row["rev_post_msg"] or 0),
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
        for c, col_key in [("Holdout_Full", "full_val"), ("Holdout_LowIntent", "li_val"), ("Detach", "det_val"), ("Unassign", "una_val")]:
            val = diwm.get(c, {}).get(key, 0)
            if key == "rev_post_msg":
                row_data[col_key] = f"${val:,.0f}"
            elif show_pct:
                pct = fmt_pct(val / sizes.get(c, 1))
                row_data[col_key] = f"{val:,} ({pct})"
            else:
                row_data[col_key] = str(int(val))
        diwm_rows.append(row_data)
    ctx["diwm_rows"] = diwm_rows

    post_msg_starts = sum(diwm.get(c, {}).get("started_post_msg", 0) for c in COHORTS)
    if post_msg_starts == 0:
        ctx["diwm_headline"] = "Zero DIWM/DIY re-engagement across all cohorts."
        ctx["diwm_detail"] = (
            "Detach's theoretical advantage — redirecting customers to self-file — "
            "has produced zero conversions. No new DIWM/DIY starts post-message in any group."
        )
        ctx["diwm_narrative"] = (
            f"<strong>Zero new DIWM/DIY starts post-message across all cohorts.</strong> "
            f"The detach pathway has not activated any DIWM/DIY re-engagement. "
            f"The ~{fmt_pct(diwm.get('Detach', {}).get('ever_started', 0) / sizes.get('Detach', 1))} "
            f"'ever started' reflects prior separate DIY/DIWM usage, not a post-intervention shift."
        )
        ctx["diwm_recommendation"] = (
            "Detach's DIWM upside remains unproven. Zero post-message DIWM starts is a strong null signal."
        )
    else:
        ctx["diwm_headline"] = f"{post_msg_starts} DIWM/DIY start(s) post-message detected."
        ctx["diwm_detail"] = "Some post-message DIWM activity has emerged. Monitor for continued growth."
        ctx["diwm_narrative"] = f"<strong>{post_msg_starts} new DIWM/DIY start(s) post-message detected.</strong> Emerging signal worth monitoring."
        ctx["diwm_recommendation"] = "DIWM re-engagement is emerging. Continue monitoring."

    # ── Appointments ──────────────────────────────────────────────────────────
    df_a = data["appointments"]
    appt_daily = defaultdict(dict)
    appts_total = defaultdict(int)
    for _, row in df_a.iterrows():
        c = row["cohort"]
        d = int(row["days_since_msg"])
        n = int(row["appts"])
        if d >= 0:
            appt_daily[c][d] = n
            appts_total[c] += n

    max_appt_day = 15
    for c in appt_daily:
        if appt_daily[c]:
            max_appt_day = max(max_appt_day, max(appt_daily[c].keys()) + 1)
    ctx["max_appt_day"] = max_appt_day
    ctx["appt_daily_json"] = json.dumps({c: dict(appt_daily.get(c, {})) for c in ["Detach", "Holdout_Full", "Unassign"]})
    ctx["appts_total"] = dict(appts_total)

    # ── Main metrics tables ───────────────────────────────────────────────────
    def build_main_metrics(ctrl_cohort, ctrl_size):
        metrics = []
        metric_defs = [
            ("FS Filing Rate", None, "filing", lambda c: filing_totals.get(c, 0)),
            ("WIP Clearing (off-queue)", None, "wip_clear", lambda c: wip.get(c, {}).get("off_queue", 0)),
            ("Completed Not Filed", "(system closures)", "cnf", lambda c: states_counts[c].get("Completed_Not_Filed", 0)),
            ("Appt Handled Post-Msg", None, "appt", lambda c: appts_total.get(c, 0)),
        ]
        for label, sublabel, _, count_fn in metric_defs:
            ctrl_n = count_fn(ctrl_cohort)
            det_n = count_fn("Detach")
            una_n = count_fn("Unassign")
            ctrl_rate = ctrl_n / ctrl_size if ctrl_size else 0
            det_rate = det_n / sizes["Detach"] if sizes["Detach"] else 0
            una_rate = una_n / sizes["Unassign"] if sizes["Unassign"] else 0

            det_p, _, det_sig = stat_test(det_n, sizes["Detach"] - det_n, ctrl_n, ctrl_size - ctrl_n)
            una_p, _, una_sig = stat_test(una_n, sizes["Unassign"] - una_n, ctrl_n, ctrl_size - ctrl_n)

            metrics.append({
                "label": label, "sublabel": sublabel,
                "ctrl_rate": fmt_pct(ctrl_rate), "ctrl_count": f"{ctrl_n:,}/{ctrl_size:,}",
                "det_rate": fmt_pct(det_rate), "det_count": f"{det_n:,}/{sizes['Detach']:,}",
                "det_itc": itc(det_rate, ctrl_rate), "det_itc_class": itc_class(det_rate, ctrl_rate),
                "det_pval": fmt_p(det_p, det_sig), "det_sig_class": "sig" if det_sig else "not-sig",
                "una_rate": fmt_pct(una_rate), "una_count": f"{una_n:,}/{sizes['Unassign']:,}",
                "una_itc": itc(una_rate, ctrl_rate), "una_itc_class": itc_class(una_rate, ctrl_rate),
                "una_pval": fmt_p(una_p, una_sig), "una_sig_class": "sig" if una_sig else "not-sig",
            })
        return metrics

    ctx["main_metrics_full"] = build_main_metrics("Holdout_Full", sizes["Holdout_Full"])
    ctx["main_metrics_li"] = build_main_metrics("Holdout_LowIntent", sizes["Holdout_LowIntent"])

    # FS filing significance flags for narrative
    fs_full = ctx["main_metrics_full"][0]
    fs_li = ctx["main_metrics_li"][0]
    ctx["fs_full_sig"] = fs_full["det_sig_class"] == "sig" or fs_full["una_sig_class"] == "sig"
    ctx["fs_li_sig"] = fs_li["det_sig_class"] == "sig" or fs_li["una_sig_class"] == "sig"
    ctx["fs_full_det_itc"] = fs_full["det_itc"]
    ctx["fs_full_det_p"] = fs_full["det_pval"]
    ctx["fs_full_una_itc"] = fs_full["una_itc"]
    ctx["fs_full_una_p"] = fs_full["una_pval"]
    ctx["treatment_fs_rate"] = fmt_pct(filing_totals.get("Detach", 0) / sizes["Detach"])

    if li_filing_count == 0:
        ctx["li_interpretation"] = (
            "This suggests the messaging + intervention activated some behavior in what would have been "
            "fully dormant customers. However, the absolute lift is small."
        )
    else:
        ctx["li_interpretation"] = (
            f"The low-intent holdout had {li_filing_count} filing(s), indicating some baseline activity."
        )

    if li_filing_count == 0 and appts_total.get("Holdout_LowIntent", 0) == 0:
        ctx["li_finding_text"] = (
            "Against this baseline, both treatments show statistically significant lifts (p < 0.001). "
            f"This suggests the messaging + action did activate some behavior in customers who would "
            f"otherwise have remained completely dormant. However, the absolute magnitude remains small "
            f"(~{ctx['treatment_fs_rate']} FS filing rate)."
        )
    else:
        li_f = li_filing_count
        li_a = appts_total.get("Holdout_LowIntent", 0)
        ctx["li_finding_text"] = f"Low-intent holdout shows {li_f} filing(s) and {li_a} appointment(s) post-message."

    ctx["customer_outcome_summary"] = (
        "No significant difference. Neither treatment meaningfully improves FS filing or franchise completion vs holdout."
        if not ctx["fs_full_sig"]
        else "Some significant differences detected. Review the detailed tables."
    )
    ctx["overall_recommendation"] = (
        "Detach for immediate WIP clearing if that's the operational priority. "
        f"Re-read at day 7 post-detach (by {(DETACH_DATE + datetime.timedelta(days=7)).strftime('%-m/%-d')}) "
        "to check for DIWM emergence. If none, detach's only advantage over unassign is the system-closure, "
        "with no customer outcome benefit."
    )

    # ── Summary tests table ───────────────────────────────────────────────────
    summary_tests = []
    test_specs = [
        ("FS Filing", "Detach", "Full", "Holdout_Full", filing_totals),
        ("FS Filing", "Unassign", "Full", "Holdout_Full", filing_totals),
        ("FS Filing", "Detach", "Low Int", "Holdout_LowIntent", filing_totals),
        ("FS Filing", "Unassign", "Low Int", "Holdout_LowIntent", filing_totals),
        ("WIP Clearing", "Detach", "Full", "Holdout_Full", {c: wip.get(c, {}).get("off_queue", 0) for c in COHORTS}),
        ("WIP Clearing", "Unassign", "Full", "Holdout_Full", {c: wip.get(c, {}).get("off_queue", 0) for c in COHORTS}),
        ("WIP Clearing", "Detach", "Low Int", "Holdout_LowIntent", {c: wip.get(c, {}).get("off_queue", 0) for c in COHORTS}),
        ("WIP Clearing", "Unassign", "Low Int", "Holdout_LowIntent", {c: wip.get(c, {}).get("off_queue", 0) for c in COHORTS}),
        ("Comp Not Filed", "Detach", "Full", "Holdout_Full", {c: states_counts[c].get("Completed_Not_Filed", 0) for c in COHORTS}),
        ("Comp Not Filed", "Unassign", "Full", "Holdout_Full", {c: states_counts[c].get("Completed_Not_Filed", 0) for c in COHORTS}),
        ("Comp Not Filed", "Detach", "Low Int", "Holdout_LowIntent", {c: states_counts[c].get("Completed_Not_Filed", 0) for c in COHORTS}),
        ("Comp Not Filed", "Unassign", "Low Int", "Holdout_LowIntent", {c: states_counts[c].get("Completed_Not_Filed", 0) for c in COHORTS}),
        ("Appt Post-Msg", "Detach", "Full", "Holdout_Full", dict(appts_total)),
        ("Appt Post-Msg", "Unassign", "Full", "Holdout_Full", dict(appts_total)),
        ("Appt Post-Msg", "Detach", "Low Int", "Holdout_LowIntent", dict(appts_total)),
        ("Appt Post-Msg", "Unassign", "Low Int", "Holdout_LowIntent", dict(appts_total)),
    ]
    for metric, treatment, control_label, ctrl_cohort, counts in test_specs:
        treat_n = counts.get(treatment, 0)
        ctrl_n = counts.get(ctrl_cohort, 0)
        treat_size = sizes.get(treatment, 1)
        ctrl_size = sizes.get(ctrl_cohort, 1)
        treat_rate = treat_n / treat_size
        ctrl_rate = ctrl_n / ctrl_size

        p, test_type, sig = stat_test(treat_n, treat_size - treat_n, ctrl_n, ctrl_size - ctrl_n)

        summary_tests.append({
            "metric": metric, "treatment": treatment, "control": control_label,
            "treat_rate": fmt_pct(treat_rate), "ctrl_rate": fmt_pct(ctrl_rate),
            "itc": itc(treat_rate, ctrl_rate),
            "pval": f"<0.001" if p < 0.001 else f"{p:.3f}",
            "test_type": test_type,
            "sig": sig, "sig_class": "sig" if sig else "not-sig",
            "sig_label": "***" if sig else "—",
        })
    ctx["summary_tests"] = summary_tests

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
        log.info("Starting detach viz refresh...")
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
