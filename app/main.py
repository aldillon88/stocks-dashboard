import streamlit as st

def main():
    st.set_page_config(page_title="INSERT_APP_TITLE", layout="wide")

    st.title('INSERT_PAGE_TITLE')

    analysis_page = st.Page("page_1.py", title="Analysis")
    project_page = st.Page("page_2.py", title="Project Description")

    pg = st.navigation([analysis_page, project_page])

    pg.run()

if __name__ == '__main__':
    main()