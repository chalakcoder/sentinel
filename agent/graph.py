"""
LangGraph fraud investigation state machine.

Graph structure:
    START
      │
      ▼
  fetch_context   ← retrieves transaction history + similar cases in parallel
      │
      ▼
  assess_risk     ← IBM WatsonX LLM decides BLOCK / FLAG / APPROVE
      │
      ├──[BLOCK or FLAG or risk_score ≥ 0.85]──► investigate
      │                                               │
      │                                          take_action  ← calls block/notify/file tools
      │                                               │
      │                                            explain    ← plain-language WatsonX explanation
      │                                               │
      └──[APPROVE]──────────────────────────────► approve
                                                      │
                                                write_decision ← structured output to Kafka
                                                      │
                                                     END
"""
import json
import os
import time
import logging
from typing import TypedDict, Literal, Any

from langgraph.graph import StateGraph, START, END
from dotenv import load_dotenv

from agent.tools import (
    ALL_TOOLS,
    get_transaction_history,
    search_similar_fraud,
    block_transaction,
    send_notification,
    file_fraud_case,
)
from agent.prompts import SYSTEM_PROMPT, INVESTIGATION_PROMPT

load_dotenv()
logger = logging.getLogger(__name__)


# ── IBM WatsonX client ────────────────────────────────────────

def _get_watsonx():
    from ibm_watsonx_ai import Credentials
    from ibm_watsonx_ai.foundation_models import ModelInference
    from ibm_watsonx_ai.metanames import GenTextParamsMetaNames as GenParams

    credentials = Credentials(
        url=os.environ["WATSONX_URL"],
        api_key=os.environ["WATSONX_API_KEY"],
    )
    return ModelInference(
        model_id=os.environ.get("WATSONX_MODEL_ID", "ibm/granite-13b-instruct-v2"),
        credentials=credentials,
        project_id=os.environ["WATSONX_PROJECT_ID"],
        params={
            GenParams.MAX_NEW_TOKENS: 1024,
            GenParams.TEMPERATURE:    0.1,
            GenParams.TOP_P:          0.9,
            GenParams.STOP_SEQUENCES: ["</s>", "\n\nHuman:"],
        },
    )


def _watsonx_generate(prompt: str) -> str:
    """Call WatsonX and return generated text, with graceful fallback."""
    try:
        model    = _get_watsonx()
        response = model.generate_text(prompt=prompt)
        return response if isinstance(response, str) else str(response)
    except Exception as exc:
        logger.error("WatsonX generation failed: %s", exc)
        # Fallback: rule-based decision from risk score
        return "VERDICT: FLAG — WatsonX unavailable, using rule-based fallback."


# ── Agent State ───────────────────────────────────────────────

class AgentState(TypedDict):
    alert:               dict       # Raw FraudAlert JSON from Kafka
    transaction_history: str        # Output of get_transaction_history tool
    similar_cases:       str        # Output of search_similar_fraud tool
    risk_assessment:     str        # WatsonX initial assessment text
    action_taken:        str        # BLOCK / FLAG / APPROVE / ESCALATE
    explanation:         str        # Plain-language decision explanation
    agent_steps:         list[str]  # Execution trace for audit
    decision:            dict       # Final structured decision output
    start_time:          float      # For latency tracking


# ── Graph Nodes ───────────────────────────────────────────────

async def fetch_context(state: AgentState) -> dict:
    """Fetch transaction history and similar fraud cases."""
    alert   = state["alert"]
    user_id = alert["user_id"]

    history = await get_transaction_history.ainvoke(user_id)

    features_desc = (
        f"{'velocity fraud, ' if alert.get('velocity_flag') else ''}"
        f"{'geo jump fraud, ' if alert.get('geo_jump_flag') else ''}"
        f"merchant_category={alert.get('merchant_category', '')}, "
        f"city={alert.get('city', '')}, "
        f"risk_score={alert.get('risk_score', 0):.2f}, "
        f"amount_ratio={alert.get('amount_ratio', 1):.1f}x, "
        f"txn_count_5min={alert.get('txn_count_5min', 1)}"
    )
    similar = await search_similar_fraud.ainvoke(features_desc)

    steps = state.get("agent_steps", [])
    steps.append(f"fetch_context: retrieved history for {user_id}, found similar cases")

    return {
        "transaction_history": history,
        "similar_cases":       similar,
        "agent_steps":         steps,
    }


def assess_risk(state: AgentState) -> dict:
    """WatsonX LLM initial risk assessment — determines routing."""
    alert = state["alert"]

    # Safe format: handle missing fields with defaults
    alert_reasons_str = ", ".join(alert.get("alert_reasons", [])) or "NONE"
    prompt = SYSTEM_PROMPT + "\n\n" + INVESTIGATION_PROMPT.format(
        alert_id=alert.get("alert_id", "N/A"),
        transaction_id=alert.get("transaction_id", "N/A"),
        user_id=alert.get("user_id", "N/A"),
        amount=float(alert.get("amount", 0)),
        merchant_category=alert.get("merchant_category", "UNKNOWN"),
        city=alert.get("city", "UNKNOWN"),
        risk_score=float(alert.get("risk_score", 0)),
        velocity_flag=alert.get("velocity_flag", False),
        geo_jump_flag=alert.get("geo_jump_flag", False),
        txn_count_5min=alert.get("txn_count_5min", 1),
        txn_sum_5min=float(alert.get("txn_sum_5min", 0)),
        amount_ratio=float(alert.get("amount_ratio", 1)),
        geo_deviation_km=float(alert.get("geo_deviation_km", 0)),
        home_city=alert.get("home_city", "UNKNOWN"),
        account_age_days=alert.get("account_age_days", 365),
        customer_risk_tier=alert.get("customer_risk_tier", "LOW"),
        alert_reasons=alert_reasons_str,
    )
    prompt += (
        f"\n\nTransaction History:\n{state['transaction_history']}"
        f"\n\nSimilar Past Cases:\n{state['similar_cases']}"
        "\n\nBased on all evidence, give your initial assessment. "
        "Reply with VERDICT: BLOCK, VERDICT: FLAG, or VERDICT: APPROVE "
        "followed by 1-2 sentences of reasoning."
    )

    response = _watsonx_generate(prompt)

    steps = state.get("agent_steps", [])
    steps.append(f"assess_risk: WatsonX verdict → {response[:120]}")

    return {"risk_assessment": response, "agent_steps": steps}


def route_by_risk(state: AgentState) -> Literal["investigate", "approve"]:
    """Routing function: parse LLM verdict to determine next node."""
    assessment = (state.get("risk_assessment") or "").upper()
    risk_score = float(state["alert"].get("risk_score", 0))

    if "VERDICT: BLOCK" in assessment or "VERDICT: FLAG" in assessment:
        return "investigate"
    # Force high-risk path for very high scores even if LLM said APPROVE
    if risk_score >= 0.85:
        return "investigate"
    return "approve"


async def investigate(state: AgentState) -> dict:
    """Deep investigation: refine decision, take action."""
    alert      = state["alert"]
    risk_score = float(alert.get("risk_score", 0))

    # Determine action: BLOCK for very high confidence, FLAG otherwise
    assessment = (state.get("risk_assessment") or "").upper()
    if "VERDICT: BLOCK" in assessment or risk_score >= 0.90:
        action = "BLOCK"
    elif "VERDICT: ESCALATE" in assessment:
        action = "ESCALATE"
    else:
        action = "FLAG"

    # Execute the action via tools
    if action == "BLOCK":
        await block_transaction.ainvoke(alert["transaction_id"])
        notification_msg = (
            f"ALERT: We blocked a suspicious ₹{float(alert.get('amount', 0)):,.0f} "
            f"{alert.get('merchant_category', '')} transaction in {alert.get('city', '')}. "
            "If this was you, call 1800-SENTINEL or reply YES to unblock."
        )
        await send_notification.ainvoke({
            "user_id": alert["user_id"],
            "message": notification_msg,
        })
        fraud_type = (
            "GEO_JUMP"     if alert.get("geo_jump_flag") else
            "VELOCITY"     if alert.get("velocity_flag") else
            "AMOUNT_ANOMALY"
        )
        await file_fraud_case.ainvoke({
            "transaction_id": alert["transaction_id"],
            "user_id":        alert["user_id"],
            "fraud_type":     fraud_type,
            "description":    state.get("risk_assessment", "Flagged by ML model"),
            "risk_score":     risk_score,
        })

    elif action == "FLAG":
        await send_notification.ainvoke({
            "user_id": alert["user_id"],
            "message": (
                f"Unusual activity: ₹{float(alert.get('amount', 0)):,.0f} "
                f"{alert.get('merchant_category', '')} in {alert.get('city', '')} "
                "has been flagged for review. Reply NO to cancel."
            ),
        })

    steps = state.get("agent_steps", [])
    steps.append(f"investigate: action={action}")

    return {"action_taken": action, "agent_steps": steps}


def explain(state: AgentState) -> dict:
    """Generate plain-language customer-friendly explanation."""
    alert  = state["alert"]
    action = state.get("action_taken", "FLAG")

    prompt = (
        f"You blocked/flagged a transaction. Write 2-3 plain sentences explaining "
        f"why the transaction was {action}. Use simple language a bank customer would "
        f"understand. Be specific about which risk factor triggered this.\n\n"
        f"Transaction: ₹{float(alert.get('amount', 0)):,.2f} "
        f"at {alert.get('merchant_category', '')} in {alert.get('city', '')}\n"
        f"Velocity flag: {alert.get('velocity_flag', False)}\n"
        f"Geo-jump flag: {alert.get('geo_jump_flag', False)}\n"
        f"Amount vs normal: {float(alert.get('amount_ratio', 1)):.1f}x\n"
        f"Risk score: {float(alert.get('risk_score', 0)):.0%}"
    )
    explanation = _watsonx_generate(prompt)

    steps = state.get("agent_steps", [])
    steps.append("explain: generated customer-friendly explanation")

    return {"explanation": explanation, "agent_steps": steps}


def approve(state: AgentState) -> dict:
    """Approve a transaction that doesn't meet the block/flag threshold."""
    risk_score = float(state["alert"].get("risk_score", 0))
    steps = state.get("agent_steps", [])
    steps.append("approve: transaction approved after context review")
    return {
        "action_taken": "APPROVE",
        "explanation":  (
            f"Transaction approved. Risk score {risk_score:.0%} is below block threshold "
            "and transaction context matches normal customer behaviour."
        ),
        "agent_steps": steps,
    }


def write_decision(state: AgentState) -> dict:
    """Produce the final structured decision record."""
    alert      = state["alert"]
    latency_ms = int((time.time() - state["start_time"]) * 1000)

    similar_parsed = []
    try:
        sc_data = json.loads(state.get("similar_cases") or "{}")
        for c in sc_data.get("similar_cases", []):
            similar_parsed.append(c.get("case_description", "")[:100])
    except Exception:
        pass

    confidence = {
        "BLOCK":    0.92,
        "FLAG":     0.75,
        "APPROVE":  0.85,
        "ESCALATE": 0.60,
    }.get(state.get("action_taken", "FLAG"), 0.70)

    decision = {
        "decision_id":     f"DEC-{alert.get('transaction_id', '')[:8].upper()}",
        "alert_id":        alert.get("alert_id", ""),
        "transaction_id":  alert.get("transaction_id", ""),
        "user_id":         alert.get("user_id", ""),
        "action":          state.get("action_taken", "FLAG"),
        "confidence":      confidence,
        "reasoning":       state.get("explanation", ""),
        "similar_cases":   similar_parsed,
        "agent_steps":     state.get("agent_steps", []),
        "latency_ms":      latency_ms,
        "llm_model":       os.environ.get("WATSONX_MODEL_ID", "ibm/granite-13b-instruct-v2"),
        "decided_at":      int(time.time() * 1000),
    }
    return {"decision": decision}


# ── Build the graph ───────────────────────────────────────────

def build_graph() -> Any:
    builder = StateGraph(AgentState)

    builder.add_node("fetch_context",  fetch_context)
    builder.add_node("assess_risk",    assess_risk)
    builder.add_node("investigate",    investigate)
    builder.add_node("take_action",    investigate)   # alias — already handles actions
    builder.add_node("explain",        explain)
    builder.add_node("approve",        approve)
    builder.add_node("write_decision", write_decision)

    builder.add_edge(START,            "fetch_context")
    builder.add_edge("fetch_context",  "assess_risk")
    builder.add_conditional_edges(
        "assess_risk",
        route_by_risk,
        {"investigate": "investigate", "approve": "approve"},
    )
    builder.add_edge("investigate",    "explain")
    builder.add_edge("explain",        "write_decision")
    builder.add_edge("approve",        "write_decision")
    builder.add_edge("write_decision", END)

    return builder.compile()


FRAUD_GRAPH = build_graph()
