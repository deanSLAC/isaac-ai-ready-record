"""
ISAAC Portal — Header & Footer branding components.

Renders the ISAAC logo at the top and partner/DOE logos at the bottom
of every Streamlit page using base64-encoded inline images.
"""

import base64
import os
import streamlit as st

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _img_to_base64(filename: str) -> str:
    path = os.path.join(_STATIC_DIR, filename)
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()


@st.cache_data
def _cached_logo_b64():
    return _img_to_base64("ISAAC_full_horizontal_white.png")


@st.cache_data
def _cached_partners_b64():
    return _img_to_base64("ISAAC_partners_footer_white.png")


@st.cache_data
def _cached_doe_b64():
    return _img_to_base64("DOE_White_Seal_White_Lettering_Horizontal.png")


def render_header():
    """Render the ISAAC logo at the top-left of the page."""
    logo_b64 = _cached_logo_b64()
    st.markdown(
        f"""
        <div style="display:flex; align-items:center; margin-bottom:0.5rem;">
            <img src="data:image/png;base64,{logo_b64}"
                 alt="ISAAC" style="height:48px; width:auto;">
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_footer():
    """Render partner logos and DOE logo at the bottom of the page."""
    partners_b64 = _cached_partners_b64()
    doe_b64 = _cached_doe_b64()
    st.markdown(
        f"""
        <hr style="margin-top:3rem; border:0; border-top:1px solid rgba(255,255,255,0.1);">
        <div style="text-align:center; padding:1.5rem 1rem 0.5rem;">
            <img src="data:image/png;base64,{partners_b64}"
                 alt="ISAAC Partners: SLAC, Argonne, Berkeley Lab, Brookhaven, Lawrence Livermore, Oak Ridge"
                 style="max-width:700px; width:100%; height:auto; opacity:0.85;">
        </div>
        <div style="text-align:center; padding:0.75rem 1rem 1.5rem;">
            <img src="data:image/png;base64,{doe_b64}"
                 alt="U.S. Department of Energy"
                 style="height:36px; width:auto; opacity:0.7; margin-bottom:0.5rem;">
            <p style="color:rgba(255,255,255,0.5); font-size:0.8rem; margin:0;">
                DOE BES AI Pathfinder Project
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
