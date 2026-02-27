"""
ISAAC Portal — Header & Footer branding components.

Uses st.logo() for the persistent header logo and st.image() for the
footer partner/DOE logos (reliable across all Streamlit versions).
"""

import os
import streamlit as st

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

_LOGO_PATH = os.path.join(_STATIC_DIR, "ISAAC_full_horizontal_white.png")
_PARTNERS_PATH = os.path.join(_STATIC_DIR, "ISAAC_partners_footer_white.png")
_DOE_PATH = os.path.join(_STATIC_DIR, "DOE_White_Seal_White_Lettering_Horizontal.png")


def render_header():
    """Render the ISAAC logo at the top of the page using st.logo()."""
    try:
        st.logo(_LOGO_PATH, size="large")
    except Exception:
        # Fallback for older Streamlit without st.logo()
        st.image(_LOGO_PATH, width=250)


def render_footer():
    """Render partner logos and DOE logo at the bottom of the page."""
    st.divider()
    col1, col2, col3 = st.columns([1, 3, 1])
    with col2:
        st.image(_PARTNERS_PATH, use_container_width=True)
        subcol1, subcol2, subcol3 = st.columns([2, 1, 2])
        with subcol2:
            st.image(_DOE_PATH, width=150)
