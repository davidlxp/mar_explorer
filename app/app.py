import streamlit as st
from services import ingest_excel, ingest_pr, nlq, logs

st.set_page_config(layout="wide", page_title="Tradeweb MAR Explorer")
st.title("📊 Tradeweb MAR Explorer")

# :::::: Upload Excel :::::: #
excel_file = st.sidebar.file_uploader("Upload MAR Excel", type=["xlsx"])
if excel_file:
    month = ingest_excel.handle_excel_upload(excel_file)
    st.sidebar.success(f"Excel ingested for {month}")

# :::::: Upload Press Release :::::: #
pr_file = st.sidebar.file_uploader("Upload Press Release", type=["txt"])
if pr_file:
    ingest_pr.handle_pr_upload(pr_file)
    st.sidebar.success("Press Release ingested!")

# :::::: Chat box :::::: #
st.markdown("---")
st.subheader("💬 Ask MAR")

question = st.text_input("Type your question:")
if question:
    answer, citations, confidence = nlq.handle_question(question)
    if answer:
        st.write(answer)
        st.caption(f"Citations: {citations}")
    else:
        st.warning("I couldn’t find this in the MAR or Press Release.")
    logs.log_question(question, confidence, citations)
