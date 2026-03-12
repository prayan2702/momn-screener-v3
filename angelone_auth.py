"""
angelone_auth.py
==============
Angel One Authentication Manager for Streamlit apps using TOTP.
Includes Linux case-sensitivity fix for Streamlit Cloud.

Behavior:
  - If credentials exist in st.secrets["angelone"] → auto-login silently (no form shown)
  - If secrets are missing → show a blank manual login form (no prefilled values)
"""

import pyotp
import streamlit as st

# --- FIX FOR STREAMLIT CLOUD (LINUX) CASE SENSITIVITY BUG ---
import sys
import types
import importlib.util

spec = importlib.util.find_spec("SmartApi")
if spec and "smartapi" not in sys.modules:
    mod = types.ModuleType("smartapi")
    mod.__path__ = spec.submodule_search_locations
    sys.modules["smartapi"] = mod
# -------------------------------------------------------------

from SmartApi import SmartConnect


def _do_login(api_key, client_code, password, totp_secret):
    """
    Core login logic. Returns (SmartConnect obj, None) on success,
    or (None, error_message) on failure.
    """
    try:
        obj = SmartConnect(api_key=api_key)
        totp_code = pyotp.TOTP(totp_secret).now()
        data = obj.generateSession(client_code, password, totp_code)

        if data and data.get("status"):
            return obj, None
        else:
            error_msg = data.get("message") if data else "Unknown Error"
            return None, f"Login Failed: {error_msg}"

    except Exception as e:
        return None, f"Authentication error: {e}"


def get_angelone_client(sidebar=True):
    """
    Returns authenticated SmartConnect client or None.

    Strategy:
      1. Already logged in this session → return cached client.
      2. Secrets available → auto-login silently (no UI form shown at all).
      3. No secrets → show a blank manual login form.
    """

    # ── 1. Already authenticated ──────────────────────────────────────────────
    if "angelone_client" in st.session_state:
        return st.session_state["angelone_client"]

    # ── 2. Auto-login from secrets (silent, no form) ──────────────────────────
    secrets = st.secrets.get("angelone", {})
    api_key     = secrets.get("api_key", "")
    client_code = secrets.get("client_code", "")
    password    = secrets.get("password", "")
    totp_secret = secrets.get("totp_secret", "")

    if api_key and client_code and password and totp_secret:
        # Secrets are present → auto-login without showing any form
        if "angelone_auto_login_attempted" not in st.session_state:
            st.session_state["angelone_auto_login_attempted"] = True
            with st.spinner("🔐 Connecting to Angel One..."):
                obj, err = _do_login(api_key, client_code, password, totp_secret)
            if obj:
                st.session_state["angelone_client"] = obj
                st.rerun()
            else:
                # Auto-login failed — show error but still no credentials in UI
                container = st.sidebar if sidebar else st
                container.error(f"Angel One auto-login failed: {err}")
        return st.session_state.get("angelone_client", None)

    # ── 3. No secrets → blank manual form (nothing prefilled) ─────────────────
    container = st.sidebar if sidebar else st
    container.warning("🔐 **Angel One login required.**")

    with container.form(key="angelone_login_form"):
        st.markdown("**Angel One SmartAPI Login**")

        # IMPORTANT: No `value=` param — fields are always blank
        f_api_key     = st.text_input("API Key", type="password")
        f_client_code = st.text_input("Client ID")
        f_password    = st.text_input("PIN / Password", type="password")
        f_totp_secret = st.text_input("TOTP Secret (Base32)", type="password")

        submit_btn = st.form_submit_button("🔓 Log In")

    if submit_btn:
        if not (f_api_key and f_client_code and f_password and f_totp_secret):
            container.error("All credentials are required.")
            return None

        with st.spinner("Authenticating with Angel One..."):
            obj, err = _do_login(f_api_key, f_client_code, f_password, f_totp_secret)

        if obj:
            st.session_state["angelone_client"] = obj
            container.success("✅ Angel One authenticated successfully!")
            st.rerun()
        else:
            container.error(err)

    return None


def logout_angelone():
    """Clear session to force re-login."""
    st.session_state.pop("angelone_client", None)
    st.session_state.pop("angelone_auto_login_attempted", None)
    st.rerun()
