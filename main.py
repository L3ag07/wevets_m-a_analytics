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
from src.data_access import carregar_do_parquet

# Importar a nova função simplificada de análise
from src.analysis import salvar_excel_simplificado

# Modificação para main.py
def main():
    """
    Função principal simplificada que carrega um arquivo Parquet e gera tabelas específicas.
    """
    print("=" * 80)
    print(f"Análise Simplificada - Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Define o diretório raiz
    diretorio_raiz = pathlib.Path().resolve()
    
    # Busca arquivos Parquet existentes
    arquivos_parquet = list((diretorio_raiz / "output").glob("*.parquet"))
    
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
    try:
        resposta = input("\nSelecione o número do arquivo a ser carregado (ou pressione Enter para o mais recente): ").strip()
        
        if resposta and resposta.isdigit() and 1 <= int(resposta) <= len(arquivos_parquet):
            indice = int(resposta) - 1
        else:
            indice = 0  # Usa o mais recente por padrão
        
        caminho_parquet = arquivos_parquet[indice]
        print(f"\nCarregando arquivo: {caminho_parquet}")
        
        # Carrega o arquivo Parquet
        df_vendas = carregar_do_parquet(caminho_parquet)
        
        if df_vendas is None or df_vendas.empty:
            print("ERRO: Não foi possível carregar dados do arquivo Parquet.")
            return None
        
        print(f"\nDataFrame carregado com {len(df_vendas)} registros e {len(df_vendas.columns)} colunas.")
        
        # Mostrar as colunas disponíveis
        print("\nColunas disponíveis no DataFrame:")
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
        
        # IMPORTANTE: Adicionar aqui a chamada para preparar_dados
        from src.data_processing import preparar_dados
        print("\nPreparando dados para adicionar coluna 'hora'...")
        df_vendas = preparar_dados(df_vendas)
        if df_vendas is not None and 'hora' in df_vendas.columns:
            print(f"Coluna 'hora' criada com sucesso. Média de horas: {df_vendas['hora'].mean():.2f}")
        else:
            print("AVISO: Coluna 'hora' não foi criada corretamente ou df_vendas é None.")
        
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
    except Exception as e:
        print(f"ERRO durante a execução: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    main()