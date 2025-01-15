import os

import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, Text, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DATA_BASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

Base = declarative_base()


class Surveydata(Base):
    __tablename__ = "survey_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    estado = Column(String(50))
    bibliotecas = Column(Text)
    area_atuacao = Column(String(50))
    horas_codando = Column(String(50))
    conforto_dados = Column(String(50))
    experiencia_python = Column(Integer)
    experiencia_sql = Column(Integer)
    experiencia_cloud = Column(Integer)


def try_connection() -> "engine":
    """Conecta ao banco de dados e retorna um objeto engine.

    Se houver um erro ao conectar ao banco de dados,
    uma mensagem de erro será exibida no Streamlit.

    :return: Um objeto engine ou None se houver um erro.
    """
    try:
        return create_engine(DATA_BASE_URL)
    except SQLAlchemyError as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None


def create_table_if_not_exists(engine) -> None:
    """Cria a tabela no banco de dados se ela não existe.

    Se houver um erro ao criar a tabela,
    uma mensagem de erro ser  exibida no Streamlit.

    :param engine: Um objeto engine que representa a conex o com o banco de dados.
    """
    try:
        Base.metadata.create_all(engine)
    except SQLAlchemyError as e:
        st.error(f"Erro ao criar tabela: {e}")


def save_data_to_db(session, data):
    try:
        novo_dado = Surveydata(
            estado=data["estado"],
            bibliotecas=data["bibliotecas"],
            area_atuacao=data["area_atuacao"],
            horas_codando=data["horas_codando"],
            conforto_dados=data["conforto_dados"],
            experiencia_python=data["experiencia_python"],
            experiencia_sql=data["experiencia_sql"],
            experiencia_cloud=data["experiencia_cloud"],
        )
        session.add(novo_dado)
        session.commit()
    except SQLAlchemyError as e:
        st.error(f"Erro ao salvar dados no banco de dados: {e}")
        session.rollback()


engine = try_connection()
if engine is not None:
    create_table_if_not_exists(engine)

Session = sessionmaker(bind=engine)

estados = [
    "Acre",
    "Alagoas",
    "Amapá",
    "Amazonas",
    "Bahia",
    "Ceará",
    "Distrito Federal",
    "Espírito Santo",
    "Goiás",
    "Maranhão",
    "Mato Grosso",
    "Mato Grosso do Sul",
    "Minas Gerais",
    "Pará",
    "Paraíba",
    "Paraná",
    "Pernambuco",
    "Piauí",
    "Rio de Janeiro",
    "Rio Grande do Norte",
    "Rio Grande do Sul",
    "Rondônia",
    "Roraima",
    "Santa Catarina",
    "São Paulo",
    "Sergipe",
    "Tocantins",
]

areas_de_atuacao = [
    "Analista de Dados",
    "Cienteista de Dados",
    "Engenheiro de Dados",
    ]

bibliotecas = [
    "Pandas",
    "Pydantic",
    "scikit-learn",
    "Git",
    "Pandera",
    "streamlit",
    "postgres",
    "databricks",
    "AWS",
    "Azure",
    "airflow",
    "dbt",
    "Pyspark",
    "Polars",
    "Kafka",
    "Duckdb",
    "PowerBI",
    "Excel",
    "Tableau",
    "storm",
]

horas_codando = [
    "Menos de 5",
    "5-10",
    "10-20",
    "Mais de 20",
]

conforto_dados = [
    "Desconfortável",
    "Neutro",
    "Confortável",
    "Muito Confortável",
]

with st.form("dados_enquete"):
    estado = st.selectbox("Estado", estados)
    areas_de_atuacao = st.selectbox("Area de atuacao", areas_de_atuacao)
    bibliotecas_selecionadas = st.multiselect(
        "Bibliotecas e ferramentas mais utilizadas",
        bibliotecas,
    )
    horas_estudo = st.selectbox(
        "Horas codando ao longo da semana",
        horas_codando,
    )
    conforto_dados = st.selectbox(
        "Conforto ao programar e trabalhar com dados",
        conforto_dados,
    )
    experiencia_python = st.slider("Experiência de Python", 0, 10)
    experiencia_sql = st.slider("Experiência de SQL", 0, 10)
    experiencia_cloud = st.slider("Experiência em Cloud", 0, 10)

    submit_button = st.form_submit_button("Enviar")

    if submit_button:
        novo_dado = {
            "Estado": estado,
            "Bibliotecas e ferramentas": ",".join(bibliotecas_selecionadas),
            "Área de atuação": areas_de_atuacao,
            "Horas de Estudo": horas_estudo,
            "Conforto com dados": conforto_dados,
            "Experiência de Python": experiencia_python,
            "Experiência de SQL": experiencia_sql,
            "Experiência em Cloud": experiencia_cloud,
        }
        session = Session()
        save_data_to_db(session, novo_dado)
        st.success("Dados enviados com sucesso!")
