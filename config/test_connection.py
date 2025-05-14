
import pyodbc
import pandas as pd
import sys
import traceback
from database import get_connection_string, get_azure_ad_mfa_connection_string

def testar_conexao_padrao():
    """Testa a conexão usando autenticação SQL Server padrão."""
    try:
        # Obter string de conexão padrão
        conn_string = get_connection_string()
        print(f"Tentando conectar com autenticação SQL padrão:\n{conn_string}")
        
        # Tentar estabelecer conexão
        conn = pyodbc.connect(conn_string)
        print("✅ Conexão SQL padrão estabelecida com sucesso!")
        
        # Executar query simples
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION AS Version")
        resultado = cursor.fetchone()
        print(f"\nVersão do SQL Server: {resultado.Version[:60]}...\n")
        
        # Fechar recursos
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ ERRO na conexão SQL padrão: {e}")
        traceback.print_exc(file=sys.stdout)
        return False

def testar_conexao_azure_ad():
    """Testa a conexão usando autenticação Azure AD com MFA."""
    try:
        # Obter string de conexão Azure AD
        conn_string = get_azure_ad_mfa_connection_string()
        print(f"\nTentando conectar com autenticação Azure AD:\n{conn_string}")
        
        # Tentar estabelecer conexão
        conn = pyodbc.connect(conn_string)
        print("✅ Conexão Azure AD estabelecida com sucesso!")
        
        # Executar query simples
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 5 name, create_date FROM sys.databases ORDER BY create_date DESC")
        
        # Exibir resultados
        resultados = cursor.fetchall()
        print("\nBancos de dados encontrados:")
        for db in resultados:
            print(f"  - {db.name} (criado em: {db.create_date})")
        
        # Fechar recursos
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ ERRO na conexão Azure AD: {e}")
        # Só imprimir o traceback se for um erro diferente de login interativo não suportado
        if "interactive_login" not in str(e).lower():
            traceback.print_exc(file=sys.stdout)
        else:
            print("   Este método requer uma interface gráfica para login. Não é suportado em ambientes sem GUI.")
        return False

def testar_conexao_odbc_direta():
    """Testa a conexão usando string de conexão ODBC direta."""
    try:
        # String de conexão fixa para teste
        direct_conn_string = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=tcp:sql-databases-prd-001.database.windows.net,1433;"
            "Database=sqldb-hospwevets-prd-001;"
            "Uid=lsouza;"
            "Pwd=Qfuvojb0706;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        
        print(f"\nTentando conectar com string ODBC direta:\n{direct_conn_string}")
        
        # Tentar estabelecer conexão
        conn = pyodbc.connect(direct_conn_string)
        print("✅ Conexão ODBC direta estabelecida com sucesso!")
        
        # Executar query simples
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 5 TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")
        
        # Converter resultados para DataFrame para melhor visualização
        columns = [column[0] for column in cursor.description]
        dados = cursor.fetchall()
        df = pd.DataFrame.from_records(dados, columns=columns)
        
        print("\nTOP 5 tabelas encontradas:")
        print(df.to_string(index=False))
        
        # Fechar recursos
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ ERRO na conexão ODBC direta: {e}")
        traceback.print_exc(file=sys.stdout)
        return False

if __name__ == "__main__":
    print("=" * 80)
    print("TESTE DE CONEXÃO COM SQL SERVER NA AZURE")
    print("=" * 80)
    
    # Tentar todos os métodos de conexão
    sql_ok = testar_conexao_padrao()
    azure_ok = testar_conexao_azure_ad()
    odbc_ok = testar_conexao_odbc_direta()
    
    print("\n" + "=" * 80)
    print("RESULTADO DOS TESTES DE CONEXÃO")
    print("=" * 80)
    print(f"SQL Server padrão: {'SUCESSO ✅' if sql_ok else 'FALHA ❌'}")
    print(f"Azure AD com MFA: {'SUCESSO ✅' if azure_ok else 'FALHA ❌'}")
    print(f"ODBC string direta: {'SUCESSO ✅' if odbc_ok else 'FALHA ❌'}")
    print("=" * 80)
    
    if not (sql_ok or azure_ok or odbc_ok):
        print("\n❌ ERRO: Nenhum dos métodos de conexão funcionou.")
        print("Verifique suas credenciais e permissões no SQL Azure.")
    else:
        print("\n✅ SUCESSO: Pelo menos um método de conexão funcionou!")
        print("Você pode usar o método que funcionou em seu código.")