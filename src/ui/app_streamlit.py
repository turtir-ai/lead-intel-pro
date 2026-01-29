import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="ALMANYA Lead Intel Review", layout="wide")

st.title("B2B Lead Intel - Review Queue")

LEADS_FILE = "outputs/crm/targets_master.csv"
DECISIONS_FILE = "outputs/review_decisions.csv"

if not os.path.exists(LEADS_FILE):
    st.warning("No leads found. Please run the harvest pipeline first.")
else:
    df = pd.read_csv(LEADS_FILE)
    
    if 'reviewed' not in df.columns:
        df['reviewed'] = False
        df['decision'] = 'pending'

    st.sidebar.header("Statistics")
    st.sidebar.write(f"Total Leads: {len(df)}")
    st.sidebar.write(f"Pending: {len(df[df['reviewed'] == False])}")

    pending_leads = df[df['reviewed'] == False]

    if not pending_leads.empty:
        current_lead = pending_leads.iloc[0]
        
        st.header(f"Company: {current_lead['company']}")
        st.write(f"**Score:** {current_lead['score']}")
        st.write(f"**Source:** {current_lead.get('source', '')}")
        st.write(f"**Competitor:** {current_lead.get('competitor', '')}")
        st.write(f"**Emails:** {current_lead.get('emails', '')}")
        st.write(f"**Phones:** {current_lead.get('phones', '')}")
        st.write(f"**Websites:** {current_lead.get('websites', '')}")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("✅ Approve", use_container_width=True):
                df.at[current_lead.name, 'reviewed'] = True
                df.at[current_lead.name, 'decision'] = 'approved'
                df.to_csv(LEADS_FILE, index=False)
                st.rerun()
                
        with col2:
            if st.button("❌ Reject", use_container_width=True):
                df.at[current_lead.name, 'reviewed'] = True
                df.at[current_lead.name, 'decision'] = 'rejected'
                df.to_csv(LEADS_FILE, index=False)
                st.rerun()
                
        with col3:
            if st.button("⏩ Skip", use_container_width=True):
                # Just move to next (not marking as reviewed)
                st.info("Skipped.")
    else:
        st.success("All leads reviewed!")

st.divider()
st.subheader("Approved Leads")
if os.path.exists(LEADS_FILE):
    approved = df[df['decision'] == 'approved']
    st.dataframe(approved)
