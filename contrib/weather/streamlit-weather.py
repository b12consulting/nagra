from pathlib import Path

from nagra import Transaction, Table, load_schema
from pandas import DataFrame
import streamlit as st


DB = "sqlite://weather.db"
here = Path(__file__).parent

@st.cache_data
def init():
    load_schema(here / "weather_schema.toml")


def main():
    cities = Table.get("city").select("name").orderby("name")
    name = st.selectbox("City", [c for c, in cities])
    select =  Table.get("weather").select(
        "timestamp",
        "temperature",
        "wind_speed",
    ).where(
        '(= city.name {})'
    ).orderby("timestamp")
    rows = select.execute(name)
    df = DataFrame(rows, columns=select.columns)
    st.dataframe(df)


with Transaction(DB):
    init()
    main()
