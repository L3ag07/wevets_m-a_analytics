# Projeto de Análise de Vendas

Este projeto realiza a extração, processamento e análise de dados de vendas de um banco de dados SQL Server na Azure.

## Estrutura do Projeto

```text
projeto/
├── config/
│   └── database.py
├── querys/
│   └── new/
│       ├── gv_vendas.sql
│       └── gv_internacao.sql
├── src/
│   ├── __init__.py
│   ├── data_access.py
│   ├── data_processing.py
│   └── analysis.py
├── output/
│   └── .gitkeep
├── main.py
└── README.md
```

## Requisitos

```python
pip install pyodbc pandas
```
