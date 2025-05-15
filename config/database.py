"""
Arquivo de configuração para acesso ao banco de dados SQL Server na Azure usando Microsoft Entra.
"""

# Configurações da conexão com o SQL Server usando Microsoft Entra (Azure AD)
DB_CONFIG = {
    'server': 'sql-databases-prd-001.database.windows.net',
    'database': 'sqldb-wevets-prd-001',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

def get_connection_string():
    """Retorna a string de conexão formatada usando Azure AD com MFA."""
    
    # String de conexão usando autenticação Azure AD Interactive
    conn_string = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Authentication=ActiveDirectoryInteractive;"
        f"UID=lsouza@wevets.com.br"
    )
    
    return conn_string


def get_sql_auth_connection_string():
    """
    Retorna a string de conexão formatada usando SQL Server Authentication.
    
    Returns:
        str: String de conexão formatada
    """
    
    # String de conexão usando autenticação SQL Server
    conn_string = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID=guruvet-operator;"
        f"PWD=viJ6588bSV9myBk4UQXe;"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    
    return conn_string

# Este bloco é executado apenas quando o arquivo é executado diretamente
if __name__ == "__main__":
    print("String de conexão Azure AD com MFA:")
    print(get_connection_string())