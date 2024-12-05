import streamlit as st


def main():
    st.set_page_config(page_title="Stock Portfolio Dashboard", layout="wide")

    st.title('Stock Portfolio Dashboard')

    analysis_page = st.Page("page_1.py", title="Analysis")
    project_page = st.Page("page_2.py", title="vs. S&P 500")

    pg = st.navigation([analysis_page, project_page])

    pg.run()

if __name__ == '__main__':
    main()