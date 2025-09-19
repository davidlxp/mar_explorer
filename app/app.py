import streamlit as st
from services import task_handle_mar, task_handle_pr, nlq, logs

st.set_page_config(layout="wide", page_title="Tradeweb MAR Explorer")
st.title("📊 Tradeweb MAR Explorer")

# :::::: Upload Excel :::::: #
excel_file = st.sidebar.file_uploader("Upload MAR Excel", type=["xlsx"])
if excel_file:
    try:
        month = task_handle_mar.handle_mar_update(excel_file)
        st.sidebar.success(f"Excel ingested!")
    except Exception as e:
        import traceback
        st.error(f"Error: {e}")
        st.text(traceback.format_exc())

# :::::: Upload Press Release :::::: #
pr_file = st.sidebar.file_uploader("Upload Press Release", type=["txt"])
if pr_file:
    task_handle_pr.handle_pr_upload(pr_file)
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
