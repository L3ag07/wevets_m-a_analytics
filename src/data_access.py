"""
Módulo responsável pelo acesso aos dados no SQL Server com autenticação Microsoft Entra.
"""

import sys
import traceback
import pyodbc
import pandas as pd
import pathlib
from datetime import datetime
from config.database import get_connection_string

def estabelecer_conexao():
    """
    Estabelece conexão com o banco de dados SQL Server na Azure usando autenticação Azure AD com MFA.
    
    Returns:
        pyodbc.Connection: Objeto de conexão com o banco de dados.
    """
    try:
        # Obter string de conexão Azure AD com MFA
        conn_string = get_connection_string()
        print(f"Tentando conectar com autenticação Azure AD:\n{conn_string}")
        
        # Tentar estabelecer conexão
        conn = pyodbc.connect(conn_string)
        print("Conexão Azure AD estabelecida com sucesso!")
        
        return conn
    except Exception as e:
        print(f"ERRO na conexão Azure AD: {e}")
        # Só imprimir o traceback se for um erro diferente de login interativo não suportado
        if "interactive_login" not in str(e).lower():
            traceback.print_exc(file=sys.stdout)
        else:
            print("   Este método requer uma interface gráfica para login. Não é suportado em ambientes sem GUI.")
        return None

def ler_arquivo_query(caminho_arquivo):
    """
    Lê o conteúdo de um arquivo SQL.
    
    Args:
        caminho_arquivo (pathlib.Path ou str): Caminho para o arquivo SQL.
        
    Returns:
        str: Conteúdo do arquivo SQL.
    """
    try:
        # Converte para objeto Path se for string
        caminho = pathlib.Path(caminho_arquivo)
        
        # Garante que o diretório pai existe
        if not caminho.parent.exists():
            caminho.parent.mkdir(parents=True, exist_ok=True)
            print(f"Diretório criado: {caminho.parent}")
            
        # Tenta abrir o arquivo
        if not caminho.exists():
            print(f"ERRO: O arquivo {caminho} não existe!")
            return None
            
        # Lê o conteúdo do arquivo
        conteudo = caminho.read_text(encoding='utf-8')
        print(f"Arquivo SQL lido com sucesso: {caminho}")
        return conteudo
    except Exception as e:
        print(f"Erro ao ler o arquivo SQL: {e}")
        return None

def executar_query(conn, query):
    """
    Executa uma query SQL e retorna os resultados como um DataFrame.
    Otimizado para grandes conjuntos de dados.
    
    Args:
        conn (pyodbc.Connection): Objeto de conexão com o banco de dados.
        query (str): Query SQL a ser executada.
        
    Returns:
        pandas.DataFrame: DataFrame com os resultados da query.
    """
    try:
        print("Iniciando execução da query...")
        
        # Usar cursor para executar a query
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Verificar se temos resultados
        if cursor.description is None:
            print("A query não retornou colunas")
            return pd.DataFrame()
        
        # Obter nomes das colunas
        columns = [column[0] for column in cursor.description]
        print(f"Colunas detectadas: {len(columns)}")
        
        # Processar resultados em lotes
        print("Processando resultados em lotes...")
        all_data = []
        batch_size = 50000
        total_rows = 0
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
                
            # Converter cada linha para uma lista (mais eficiente que dicionário)
            batch_data = [list(row) for row in rows]
            all_data.extend(batch_data)
            
            total_rows += len(rows)
            print(f"Processados {total_rows} registros até o momento")
        
        print(f"Total de registros: {total_rows}")
        
        # Criar DataFrame 
        df = pd.DataFrame(all_data, columns=columns)
        
        print(f"DataFrame criado com sucesso. Dimensões: {df.shape}")
        return df
    except Exception as e:
        print(f"Erro ao executar a query: {e}")
        traceback.print_exc(file=sys.stdout)
        return None

def salvar_como_parquet(df, nome_arquivo=None):
    """
    Salva o DataFrame em formato Parquet para acesso eficiente.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser salvo.
        nome_arquivo (str, opcional): Nome do arquivo sem extensão.
        
    Returns:
        pathlib.Path: Caminho para o arquivo salvo.
    """
    try:
        # Instalar pyarrow se necessário
        try:
            import pyarrow
        except ImportError:
            print("Instalando dependências necessárias...")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pyarrow"])
            import pyarrow
        
        # Criar diretório de saída se não existir
        diretorio_saida = pathlib.Path().resolve() / "output"
        diretorio_saida.mkdir(parents=True, exist_ok=True)
        
        # Gerar nome de arquivo com timestamp se não fornecido
        if nome_arquivo is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_arquivo = f"dados_vendas_{timestamp}"
        
        # Caminho completo do arquivo
        caminho_arquivo = diretorio_saida / f"{nome_arquivo}.parquet"
        
        # Salvar como Parquet
        print(f"Salvando DataFrame em formato Parquet: {caminho_arquivo}")
        df.to_parquet(caminho_arquivo, engine='pyarrow', compression='snappy')
        
        print(f"Arquivo salvo com sucesso ({caminho_arquivo.stat().st_size / (1024*1024):.2f} MB)")
        return caminho_arquivo
    except Exception as e:
        print(f"Erro ao salvar arquivo Parquet: {e}")
        traceback.print_exc(file=sys.stdout)
        return None

def buscar_dados_vendas(caminho_query=None, salvar_parquet=True):
    """
    Função principal para buscar dados de vendas do banco de dados.
    
    Args:
        caminho_query (pathlib.Path ou str, opcional): Caminho para o arquivo da query.
        salvar_parquet (bool): Se True, salva os dados em formato Parquet.
        
    Returns:
        pandas.DataFrame ou tuple: DataFrame com os dados ou tupla (DataFrame, caminho_parquet).
    """
    # Define o caminho padrão da query se não for fornecido
    if caminho_query is None:
        caminho_query = pathlib.Path().resolve() / "querys" / "new" / "gv_vendas.sql"
    
    conn = estabelecer_conexao()
    if conn is None:
        return None
        
    try:
        query = ler_arquivo_query(caminho_query)
        if query is None:
            conn.close()
            return None
            
        df_vendas = executar_query(conn, query)
        
        if df_vendas is None or df_vendas.empty:
            print("Não foram encontrados dados de vendas.")
            return df_vendas  # Retorna None ou DataFrame vazio
            
        print(f"Dados de vendas recuperados com sucesso: {len(df_vendas)} registros")
        
        # Salvar como Parquet se solicitado
        if salvar_parquet:
            caminho_parquet = salvar_como_parquet(df_vendas)
            if caminho_parquet:
                print(f"Para futuras análises, utilize o arquivo: {caminho_parquet}")
                return df_vendas, caminho_parquet
        
        return df_vendas
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

def carregar_do_parquet(caminho_arquivo):
    """
    Carrega um DataFrame a partir de um arquivo Parquet.
    
    Args:
        caminho_arquivo (pathlib.Path ou str): Caminho para o arquivo Parquet.
        
    Returns:
        pandas.DataFrame: DataFrame carregado do arquivo.
    """
    try:
        caminho = pathlib.Path(caminho_arquivo)
        if not caminho.exists():
            print(f"ERRO: Arquivo não encontrado: {caminho}")
            return None
            
        print(f"Carregando dados do arquivo Parquet: {caminho}")
        df = pd.read_parquet(caminho)
        
        print(f"Dados carregados com sucesso: {len(df)} registros, {len(df.columns)} colunas")
        return df
    except Exception as e:
        print(f"Erro ao carregar arquivo Parquet: {e}")
        traceback.print_exc(file=sys.stdout)
        return None

def otimizar_dataframe(df):
    """
    Otimiza o uso de memória do DataFrame reduzindo o tipo de dados.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser otimizado.
        
    Returns:
        pandas.DataFrame: DataFrame otimizado.
    """
    try:
        inicio_memoria = df.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"Uso de memória antes da otimização: {inicio_memoria:.2f} MB")
        
        # Cópia do DataFrame para evitar warnings
        df_otimizado = df.copy()
        
        # Otimização de tipos numéricos
        for col in df_otimizado.select_dtypes(include=['int64', 'float64']).columns:
            # Inteiros
            if pd.api.types.is_integer_dtype(df_otimizado[col]):
                # Tratamento de NaN em colunas inteiras
                if df_otimizado[col].isna().any():
                    df_otimizado[col] = df_otimizado[col].astype('float32')
                else:
                    max_val = df_otimizado[col].max()
                    min_val = df_otimizado[col].min()
                    
                    if min_val >= 0:  # Valores não-negativos
                        if max_val < 255:
                            df_otimizado[col] = df_otimizado[col].astype('uint8')
                        elif max_val < 65535:
                            df_otimizado[col] = df_otimizado[col].astype('uint16')
                        elif max_val < 4294967295:
                            df_otimizado[col] = df_otimizado[col].astype('uint32')
                    else:  # Valores com sinal
                        if min_val > -128 and max_val < 127:
                            df_otimizado[col] = df_otimizado[col].astype('int8')
                        elif min_val > -32768 and max_val < 32767:
                            df_otimizado[col] = df_otimizado[col].astype('int16')
                        elif min_val > -2147483648 and max_val < 2147483647:
                            df_otimizado[col] = df_otimizado[col].astype('int32')
            
            # Floats
            elif pd.api.types.is_float_dtype(df_otimizado[col]):
                df_otimizado[col] = df_otimizado[col].astype('float32')
        
        # Otimização de strings para categorias
        for col in df_otimizado.select_dtypes(include=['object']).columns:
            num_unique = df_otimizado[col].nunique()
            num_total = len(df_otimizado[col])
            
            # Se menos de 50% são valores únicos, converter para categoria
            if num_unique / num_total < 0.5:
                df_otimizado[col] = df_otimizado[col].astype('category')
        
        # Calcular economia de memória
        fim_memoria = df_otimizado.memory_usage(deep=True).sum() / (1024 * 1024)
        economia = (inicio_memoria - fim_memoria) / inicio_memoria * 100
        
        print(f"Uso de memória após otimização: {fim_memoria:.2f} MB")
        print(f"Economia de memória: {economia:.2f}% ({inicio_memoria - fim_memoria:.2f} MB)")
        
        return df_otimizado
    except Exception as e:
        print(f"Erro ao otimizar DataFrame: {e}")
        traceback.print_exc(file=sys.stdout)
        return df  # Retorna o DataFrame original em caso de erro