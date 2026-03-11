"""
angelone_auth.py
==============
Angel One Authentication Manager for Streamlit apps using TOTP.
Includes Linux case-sensitivity fix for Streamlit Cloud.
"""

import pyotp
import streamlit as st

# --- FIX FOR STREAMLIT CLOUD (LINUX) CASE SENSITIVITY BUG ---
# Angel One ki library galti se 'smartapi' dhoondhti hai, jabki install 'SmartApi' hota hai.
# Ye code Linux ko dono ko ek hi samajhne ke liye force karta hai.
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

def get_angelone_client(sidebar=True):
    """
    Shows a sidebar form to log into Angel One using Client ID, PIN, and TOTP Secret.
    Returns the authenticated SmartConnect client object or None.
    """
    if "angelone_client" in st.session_state:
        return st.session_state["angelone_client"]

    container = st.sidebar if sidebar else st
    container.warning("🔐 **Angel One token not found.** Please log in.")

    with container.form(key="angelone_login_form"):
        st.markdown("**Angel One SmartAPI Login**")
        
        api_key = st.text_input("API Key", value=st.secrets.get("angelone", {}).get("api_key", ""), type="password")
        client_code = st.text_input("Client ID", value=st.secrets.get("angelone", {}).get("client_code", ""))
        password = st.text_input("PIN / Password", value=st.secrets.get("angelone", {}).get("password", ""), type="password")
        totp_secret = st.text_input("TOTP Secret (Base32)", value=st.secrets.get("angelone", {}).get("totp_secret", ""), type="password")
        
        submit_btn = st.form_submit_button("🔓 Log In")

    if submit_btn:
        if not (api_key and client_code and password and totp_secret):
            container.error("All credentials are required.")
            return None
            
        try:
            with st.spinner("Authenticating with Angel One..."):
                obj = SmartConnect(api_key=api_key)
                
                # Generate dynamic TOTP code
                totp_code = pyotp.TOTP(totp_secret).now()
                
                # Fetch session tokens
                data = obj.generateSession(client_code, password, totp_code)
                
                if data and data.get('status'):
                    st.session_state["angelone_client"] = obj
                    container.success("✅ Angel One authenticated successfully!")
                    st.rerun() 
                else:
                    error_msg = data.get('message') if data else "Unknown Error"
                    container.error(f"Login Failed: {error_msg}")
                    
        except Exception as e:
            container.error(f"Authentication error: {e}")

    return None

def logout_angelone():
    """Clear session to force re-login."""
    st.session_state.pop("angelone_client", None)
    st.rerun()
