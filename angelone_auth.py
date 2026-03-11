import pyotp
import streamlit as st
from SmartApi import SmartConnect

def get_angelone_client(sidebar=True):
    if "angelone_client" in st.session_state:
        return st.session_state["angelone_client"]

    container = st.sidebar if sidebar else st
    container.warning("🔐 **Angel One token not found.** Please log in.")

    with container.form(key="angelone_login_form"):
        st.markdown("**Angel One SmartAPI Login**")
        api_key = st.text_input("API Key", value=st.secrets.get("angelone", {}).get("api_key", ""), type="password")
        client_code = st.text_input("Client ID", value=st.secrets.get("angelone", {}).get("client_code", ""))
        password = st.text_input("PIN", value=st.secrets.get("angelone", {}).get("password", ""), type="password")
        totp_secret = st.text_input("TOTP Secret", value=st.secrets.get("angelone", {}).get("totp_secret", ""), type="password")

        submit_btn = st.form_submit_button("🔓 Log In")

    if submit_btn:
        if not (api_key and client_code and password and totp_secret):
            container.error("All credentials are required.")
            return None

        try:
            with st.spinner("Authenticating with Angel One..."):
                obj = SmartConnect(api_key=api_key)
                totp_code = pyotp.TOTP(totp_secret).now()
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
