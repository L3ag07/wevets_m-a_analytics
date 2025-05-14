"""
Versão simplificada do módulo principal para carregar parquet e gerar Excel com tabelas específicas
"""

import sys
import pathlib
from datetime import datetime
import pandas as pd

# Adicionando o diretório raiz ao path para importações corretas
notebook_dir = str(pathlib.Path().resolve())
sys.path.append(notebook_dir)

# Importação das funções para acesso aos dados
from src.data_processing import classificar_vendas, preparar_dados
from src.data_access import carregar_do_parquet, buscar_dados_vendas
from src.analysis import salvar_excel_simplificado

def main():
    """
    Função principal simplificada que carrega um arquivo Parquet, classifica os dados,
    configura as horas e gera tabelas específicas.
    """
    print("=" * 80)
    print(f"Análise Simplificada - Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Define o diretório raiz
    diretorio_raiz = pathlib.Path().resolve()
    
    # Busca arquivos Parquet existentes
    arquivos_parquet = list((diretorio_raiz / "output").glob("*.parquet"))
    
    # Apresenta as opções ao usuário
    print("\nOpções disponíveis:")
    print("1. Usar arquivo Parquet existente")
    print("2. Criar novo arquivo Parquet a partir do banco de dados")
    
    opcao = input("\nEscolha uma opção (1 ou 2): ").strip()
    
    df_vendas = None
    caminho_parquet = None
    
    if opcao == "2":
        print("\n" + "=" * 80)
        print("Criando novo arquivo Parquet a partir do banco de dados...")
        print("=" * 80)
        
        # Opção para especificar um arquivo SQL personalizado
        usar_sql_personalizado = input("\nDeseja usar um arquivo SQL personalizado? (s/n): ").strip().lower()
        
        if usar_sql_personalizado == 's':
            # Listar arquivos SQL disponíveis
            diretorio_querys = diretorio_raiz / "querys" / "new"
            if diretorio_querys.exists():
                arquivos_sql = list(diretorio_querys.glob("*.sql"))
                
                if arquivos_sql:
                    print("\nArquivos SQL disponíveis:")
                    for i, arquivo in enumerate(arquivos_sql):
                        print(f"{i+1}. {arquivo.name}")
                    
                    resposta_sql = input("\nSelecione o número do arquivo SQL a ser usado (ou pressione Enter para o padrão): ").strip()
                    
                    if resposta_sql and resposta_sql.isdigit() and 1 <= int(resposta_sql) <= len(arquivos_sql):
                        caminho_sql = arquivos_sql[int(resposta_sql) - 1]
                    else:
                        caminho_sql = diretorio_raiz / "querys" / "new" / "gv_vendas.sql"
                else:
                    print("Nenhum arquivo SQL encontrado em querys/new/. Usando o padrão.")
                    caminho_sql = None
            else:
                print("Diretório querys/new/ não encontrado. Usando o arquivo SQL padrão.")
                caminho_sql = None
        else:
            caminho_sql = None
        
        try:
            # Buscar dados do banco de dados
            print("\nConectando ao banco de dados e executando a consulta...")
            resultado = buscar_dados_vendas(caminho_query=caminho_sql, salvar_parquet=True)
            
            if isinstance(resultado, tuple) and len(resultado) == 2:
                df_vendas, caminho_parquet = resultado
                print(f"\nArquivo Parquet criado com sucesso: {caminho_parquet}")
            else:
                df_vendas = resultado
                print("\nDados obtidos do banco de dados, mas o arquivo Parquet não foi salvo.")
                return None
                
        except Exception as e:
            print(f"\nERRO ao criar novo arquivo Parquet: {e}")
            import traceback
            traceback.print_exc()
            return None
            
    elif opcao == "1":
        # Verificar se existem arquivos Parquet
        if not arquivos_parquet:
            print("ERRO: Nenhum arquivo Parquet encontrado no diretório 'output'.")
            return None
        
        # Ordena por data de modificação (mais recente primeiro)
        arquivos_parquet.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Lista os arquivos Parquet disponíveis
        print("\nArquivos Parquet disponíveis:")
        for i, arquivo in enumerate(arquivos_parquet):
            print(f"{i+1}. {arquivo.name} (Modificado: {datetime.fromtimestamp(arquivo.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')})")
        
        # Seleção do arquivo a ser utilizado
        resposta = input("\nSelecione o número do arquivo a ser carregado (ou pressione Enter para o mais recente): ").strip()
        
        if resposta and resposta.isdigit() and 1 <= int(resposta) <= len(arquivos_parquet):
            indice = int(resposta) - 1
        else:
            indice = 0  # Usa o mais recente por padrão
        
        caminho_parquet = arquivos_parquet[indice]
        print(f"\nCarregando arquivo: {caminho_parquet}")
        
        # Carrega o arquivo Parquet
        df_vendas = carregar_do_parquet(caminho_parquet)
    else:
        print("Opção inválida. Saindo do programa.")
        return None
    
    # Verificar se o DataFrame foi carregado corretamente
    if df_vendas is None or df_vendas.empty:
        print("ERRO: Não foi possível carregar dados do arquivo Parquet ou do banco de dados.")
        return None
    
    print(f"\nDataFrame carregado com {len(df_vendas)} registros e {len(df_vendas.columns)} colunas.")
    
    # MODIFICADO: Chamar as funções de classificação e preparação logo após o carregamento
    print("\nClassificando vendas...")
    df_vendas = classificar_vendas(df_vendas)
    if df_vendas is None or df_vendas.empty:
        print("ERRO: Falha na classificação das vendas.")
        return None
        
    print("\nPreparando dados e configurando horas...")
    df_vendas = preparar_dados(df_vendas)
    if df_vendas is None or df_vendas.empty:
        print("ERRO: Falha na preparação dos dados.")
        return None
    
    # Mostrar as colunas disponíveis
    print("\nColunas disponíveis no DataFrame após processamento:")
    for i, coluna in enumerate(df_vendas.columns):
        print(f"{i+1}. {coluna} ({df_vendas[coluna].dtype})")
    
    # Verificar diretamente se a coluna DataExecucao é uma coluna de data
    if 'DataExecucao' in df_vendas.columns:
        print("\nVerificando coluna DataExecucao:")
        try:
            # Tentar converter DataExecucao para datetime se não for
            if not pd.api.types.is_datetime64_dtype(df_vendas['DataExecucao']):
                print("Convertendo DataExecucao para datetime...")
                # Mostrar exemplos de valores
                print(f"Exemplos de valores: {df_vendas['DataExecucao'].head(3).tolist()}")
                
                # Tentar converter
                df_vendas['DataExecucao'] = pd.to_datetime(df_vendas['DataExecucao'], errors='coerce')
                print(f"Conversão realizada. Tipo atual: {df_vendas['DataExecucao'].dtype}")
        except Exception as e:
            print(f"Erro ao converter DataExecucao: {e}")
    
    # Gerar o relatório simplificado com as tabelas solicitadas
    print("\nGerando relatório simplificado com tabelas de centro por mês e horas por mês...")
    caminho_excel = salvar_excel_simplificado(df_vendas)
    
    if caminho_excel:
        print("\n" + "=" * 80)
        print(f"Relatório simplificado gerado com sucesso: {caminho_excel}")
        print("=" * 80)
        return caminho_excel
    else:
        print("ERRO: Não foi possível gerar o relatório.")
        return None


if __name__ == "__main__":
    main()