import streamlit.web.bootstrap as bootstrap
from app import app

if __name__ == "__main__":
    bootstrap.run(app.__file__, "", [], [])