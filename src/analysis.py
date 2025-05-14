# No início do arquivo analysis.py, adicione a seguinte importação:
from src.data_processing import classificar_vendas, preparar_dados

# Você pode adicionar esta importação logo após:
import pandas as pd
import os
from datetime import datetime

def converter_colunas_data(df):
    """
    Converte todas as colunas de data para formato sem timezone.
    Versão melhorada para garantir a remoção do timezone.
    
    Args:
        df (pandas.DataFrame): DataFrame com colunas de data.
        
    Returns:
        pandas.DataFrame: DataFrame com colunas de data sem timezone.
    """
    if df is None or df.empty:
        return df
    
    df_convertido = df.copy()
    
    # Listar colunas com timezone para debug
    colunas_com_tz = []
    
    # Procurar por todas as colunas datetime
    for coluna in df_convertido.columns:
        if pd.api.types.is_datetime64_dtype(df_convertido[coluna]):
            # Verificar se a coluna tem timezone
            if hasattr(df_convertido[coluna].dt, 'tz') and df_convertido[coluna].dt.tz is not None:
                print(f"Removendo timezone da coluna {coluna}")
                colunas_com_tz.append(coluna)
                # Usando método explícito de conversão para remover timezone
                df_convertido[coluna] = df_convertido[coluna].dt.tz_localize(None)
    
    # Verificação específica para DataExecucao que está causando problemas
    if 'DataExecucao' in df_convertido.columns and pd.api.types.is_datetime64_dtype(df_convertido['DataExecucao']):
        if hasattr(df_convertido['DataExecucao'].dt, 'tz') and df_convertido['DataExecucao'].dt.tz is not None:
            print("Verificando conversão de timezone para DataExecucao usando método alternativo")
            # Usando um método alternativo se o primeiro falhar
            try:
                # Primeiro método: converter para string e depois para datetime
                df_convertido['DataExecucao'] = df_convertido['DataExecucao'].astype(str)
                df_convertido['DataExecucao'] = pd.to_datetime(df_convertido['DataExecucao'], errors='coerce', utc=False)
            except:
                # Segundo método: valor nulo
                print("Não foi possível converter DataExecucao. Substituindo por valores nulos.")
                df_convertido['DataExecucao'] = pd.NaT
    
    # Verificação final para garantir que não há timezone em nenhuma coluna
    for coluna in df_convertido.columns:
        if pd.api.types.is_datetime64_dtype(df_convertido[coluna]):
            if hasattr(df_convertido[coluna].dt, 'tz') and df_convertido[coluna].dt.tz is not None:
                print(f"ALERTA: Coluna {coluna} ainda possui timezone. Convertendo para string.")
                # Se ainda tiver timezone, converter para string como último recurso
                df_convertido[coluna] = df_convertido[coluna].astype(str)
    
    # Informar as colunas que foram convertidas
    if colunas_com_tz:
        print(f"Colunas com timezone convertidas: {', '.join(colunas_com_tz)}")
    
    return df_convertido

"""
Função criar_tabela_unidade_por_mes corrigida para garantir que Ano e Mês sejam inteiros
"""
def criar_tabela_unidade_por_mes(df):
    """
    Cria uma tabela de contagem de centros por mês.
    Versão modificada para garantir que Ano e Mês sejam valores inteiros.
    
    Args:
        df (pandas.DataFrame): DataFrame com os dados classificados.
        
    Returns:
        pandas.DataFrame: DataFrame com a contagem de centros por mês.
    """
    # Verificar se existe a coluna Centro
    if 'Centro' not in df.columns:
        print("AVISO: Coluna 'Centro' não encontrada. Verificando alternativas...")
        # Tentar encontrar colunas que possam representar centros
        possiveis_colunas = [col for col in df.columns if 'centr' in col.lower() or 'unit' in col.lower()]
        if possiveis_colunas:
            print(f"Usando coluna alternativa: {possiveis_colunas[0]}")
            centro_col = possiveis_colunas[0]
        else:
            print("Nenhuma coluna de centro encontrada. Usando 'Classificacao' como substituto.")
            centro_col = 'Classificacao'
    else:
        centro_col = 'Centro'
    
    # Garantir que Ano e Mês existam e sejam inteiros
    ano_mes_ok = True
    
    # Se já temos Ano e Mês, garantir que são inteiros
    if 'Ano' in df.columns and 'Mes' in df.columns:
        print("Verificando e convertendo Ano e Mês para inteiros...")
        try:
            # Usar astype para converter explicitamente para inteiros
            df['Ano'] = pd.to_numeric(df['Ano'], errors='coerce').fillna(2023).astype('int32')
            df['Mes'] = pd.to_numeric(df['Mes'], errors='coerce').fillna(1).astype('int32')
            print(f"Amostra de valores de Ano após conversão: {df['Ano'].head(3).tolist()}")
        except Exception as e:
            print(f"ERRO ao converter Ano e Mês para inteiros: {e}")
            ano_mes_ok = False
    else:
        ano_mes_ok = False
    
    # Se não temos Ano e Mês como inteiros, temos que criá-los
    if not ano_mes_ok:
        # Se temos DataCriacao, extrair Ano e Mês
        if 'DataCriacao' in df.columns:
            print("Processando DataCriacao para extrair Ano e Mês como inteiros...")
            
            # Se for datetime
            if pd.api.types.is_datetime64_dtype(df['DataCriacao']):
                df['Ano'] = df['DataCriacao'].dt.year.astype('int32')
                df['Mes'] = df['DataCriacao'].dt.month.astype('int32')
            else:
                # Para o formato específico '2023-03-09 04:40:17 -03'
                def extrair_ano(valor):
                    if not isinstance(valor, str):
                        return 2023
                    
                    # Remover a parte do timezone se existir
                    valor_limpo = valor.split(' -03')[0] if ' -03' in valor else valor
                    
                    # Extrair o ano (primeiros 4 caracteres)
                    try:
                        return int(valor_limpo[:4])
                    except:
                        return 2023
                
                def extrair_mes(valor):
                    if not isinstance(valor, str):
                        return 1
                    
                    # Remover a parte do timezone se existir
                    valor_limpo = valor.split(' -03')[0] if ' -03' in valor else valor
                    
                    # Extrair o mês (caracteres 5 e 6, considerando que há um hífen)
                    try:
                        return int(valor_limpo[5:7])
                    except:
                        return 1
                
                df['Ano'] = df['DataCriacao'].apply(extrair_ano)
                df['Mes'] = df['DataCriacao'].apply(extrair_mes)
        else:
            # Se não temos DataCriacao, usamos valores padrão
            print("AVISO: Não há coluna DataCriacao. Usando valores padrão para Ano e Mês.")
            df['Ano'] = 2023
            df['Mes'] = 1
    
    # Verificar se Ano e Mês estão corretos
    print(f"Tipos de dados das colunas: Ano ({df['Ano'].dtype}), Mes ({df['Mes'].dtype})")
    print(f"Primeiros valores de Ano: {df['Ano'].head(3).tolist()}")
    
    # Agrupar por centro, ano e mês para contagem - garantindo que são inteiros antes
    df['Ano'] = df['Ano'].astype('int32')
    df['Mes'] = df['Mes'].astype('int32')
    tabela_contagem = df.groupby([centro_col, 'Ano', 'Mes']).size().reset_index(name='Contagem')
    
    # Criar coluna de período para melhor visualização - COM VERIFICAÇÃO DE TIPO
    # Use str() explicitamente para converter para string antes de concatenar
    tabela_contagem['Periodo'] = tabela_contagem['Ano'].apply(lambda x: str(int(x))) + '-' + tabela_contagem['Mes'].apply(lambda x: str(int(x)).zfill(2))
    
    # Ordenar por período
    tabela_contagem = tabela_contagem.sort_values(['Ano', 'Mes', centro_col]).reset_index(drop=True)
    
    # Renomear coluna para clareza
    tabela_contagem.rename(columns={centro_col: 'Centro'}, inplace=True)
    
    print(f"Tabela de contagem por centro e mês criada com {len(tabela_contagem)} linhas.")
    return tabela_contagem


def criar_tabela_horas_por_mes(df):
    """
    Cria uma tabela de quantidade de horas por mês.
    Versão corrigida para dar prioridade à coluna 'hora' e garantir que Ano e Mês sejam valores inteiros.
    
    Args:
        df (pandas.DataFrame): DataFrame com os dados classificados.
        
    Returns:
        pandas.DataFrame: DataFrame com a quantidade de horas por mês.
    """
    # Verificar se existe coluna 'hora' primeiro
    if 'hora' in df.columns:
        print("Usando coluna 'hora' para cálculo de horas.")
        hora_col = 'hora'
    else:
        # Verificar se existem outras colunas de horas
        colunas_horas = [col for col in df.columns if 'hora' in col.lower() or 'time' in col.lower() or 'duracao' in col.lower()]
        
        if not colunas_horas:
            print("AVISO: Não foi encontrada coluna de horas. Tentando alternativa...")
            # Verificar se existe uma coluna que possa representar duração
            if 'ValorVenda' in df.columns:
                print("Usando 'ValorVenda' como substituto para demonstração.")
                hora_col = 'ValorVenda'
                
                # Tentar converter ValorVenda para numérico se não for
                if not pd.api.types.is_numeric_dtype(df[hora_col]):
                    try:
                        # Limpar a coluna ValorVenda (remover símbolos, vírgulas, etc.)
                        df[hora_col] = df[hora_col].astype(str).str.replace(',', '.').str.replace('R$', '').str.strip()
                        df[hora_col] = pd.to_numeric(df[hora_col], errors='coerce').fillna(0)
                    except Exception as e:
                        print(f"Erro ao converter ValorVenda para numérico: {e}")
            else:
                # Tentar encontrar qualquer coluna numérica
                colunas_numericas = df.select_dtypes(include=['number']).columns.tolist()
                if colunas_numericas:
                    hora_col = colunas_numericas[0]
                    print(f"Usando coluna numérica '{hora_col}' como substituto.")
                else:
                    print("ERRO: Não foi possível encontrar uma coluna numérica para representar horas.")
                    return None
        else:
            hora_col = colunas_horas[0]
            print(f"Usando coluna {hora_col} para cálculo de horas.")
    
    # Garantir que Ano e Mês existam e sejam inteiros (mesmo processo da outra função)
    ano_mes_ok = True
    
    if 'Ano' in df.columns and 'Mes' in df.columns:
        try:
            # Converter explicitamente para inteiros
            df['Ano'] = pd.to_numeric(df['Ano'], errors='coerce').fillna(2023).astype('int32')
            df['Mes'] = pd.to_numeric(df['Mes'], errors='coerce').fillna(1).astype('int32')
        except Exception as e:
            print(f"ERRO ao converter Ano e Mês para inteiros: {e}")
            ano_mes_ok = False
    else:
        ano_mes_ok = False
    
    if not ano_mes_ok:
        # Valores padrão se não conseguir converter
        df['Ano'] = 2023
        df['Mes'] = 1
    
    # Verificar se existe coluna de centro
    if 'Centro' not in df.columns:
        possiveis_colunas = [col for col in df.columns if 'centr' in col.lower() or 'unit' in col.lower()]
        if possiveis_colunas:
            centro_col = possiveis_colunas[0]
        else:
            centro_col = 'Classificacao'
    else:
        centro_col = 'Centro'
    
    # Verificar se a coluna de horas é numérica
    if not pd.api.types.is_numeric_dtype(df[hora_col]):
        try:
            print(f"Convertendo coluna {hora_col} para numérico...")
            # Tratar casos onde valores podem ter vírgula em vez de ponto
            df[hora_col] = df[hora_col].astype(str).str.replace(',', '.').str.replace('R$', '').str.strip()
            df[hora_col] = pd.to_numeric(df[hora_col], errors='coerce').fillna(0)
        except Exception as e:
            print(f"Erro ao converter {hora_col} para numérico: {e}")
            # Criar uma coluna temporária com valores aleatórios para demonstração
            import numpy as np
            df['temp_valor'] = np.random.rand(len(df)) * 100
            hora_col = 'temp_valor'
    
    # Agrupar por centro, ano e mês para soma de horas
    df['Ano'] = df['Ano'].astype('int32')  # Garantir tipo inteiro
    df['Mes'] = df['Mes'].astype('int32')  # Garantir tipo inteiro
    tabela_horas = df.groupby([centro_col, 'Ano', 'Mes']).agg({
        hora_col: 'sum'
    }).reset_index()
    
    # Criar coluna de período para melhor visualização - COM VERIFICAÇÃO DE TIPO
    tabela_horas['Periodo'] = tabela_horas['Ano'].apply(lambda x: str(int(x))) + '-' + tabela_horas['Mes'].apply(lambda x: str(int(x)).zfill(2))
    
    # Ordenar por período
    tabela_horas = tabela_horas.sort_values(['Ano', 'Mes', centro_col]).reset_index(drop=True)
    
    # Renomear colunas para clareza
    tabela_horas.rename(columns={
        centro_col: 'Centro',
        hora_col: 'Total_Horas'
    }, inplace=True)
    
    return tabela_horas


"""
Versão simplificada do salvar_excel_simplificado usando a nova função para remover timezones
"""
def salvar_excel_simplificado(df, pasta_saida='output'):
    """
    Função que salva as tabelas em Excel, com abas separadas por classificação.
    
    Args:
        df (pandas.DataFrame): DataFrame com os dados.
        pasta_saida (str): Pasta onde o arquivo será salvo.
    
    Returns:
        str: Caminho do arquivo Excel salvo.
    """
    
    tabelas_por_classificacao = {}
    # Garantindo que a pasta de saída existe
    if not os.path.exists(pasta_saida):
        os.makedirs(pasta_saida, exist_ok=True)
    
    # Criando um timestamp para os nomes de arquivo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_excel = os.path.join(pasta_saida, f'relatorio_por_classificacao_{timestamp}.xlsx')
    
    try:
        # Criar as tabelas por classificação
        print("Criando tabelas separadas por classificação (Cardiologia, Imagem, etc.)...")
        tabelas_por_classificacao = criar_tabelas_por_cluster(df)
        
        if not tabelas_por_classificacao:
            print("AVISO: Não foi possível criar tabelas por classificação. Usando método anterior...")
            # Usar o método anterior se não conseguir criar tabelas por classificação
            df_limpo = remover_todos_timezones(df)
            tabela_unidades = criar_tabela_unidade_por_mes(df_limpo)
            tabela_horas = criar_tabela_horas_por_mes(df_limpo)
            tabela_unidades_pivot = formatar_tabela_pivot(tabela_unidades, 'Contagem')
            tabela_horas_pivot = formatar_tabela_pivot(tabela_horas, 'Total_Horas')
            
            # Salvar em Excel
            with pd.ExcelWriter(arquivo_excel, engine='openpyxl') as writer:
                if tabela_unidades_pivot is not None:
                    tabela_unidades_pivot.to_excel(writer, sheet_name='Contagem_por_Periodo', index=False)
                    print("Tabela 'Contagem por Período' salva com sucesso.")
                
                if tabela_horas_pivot is not None:
                    tabela_horas_pivot.to_excel(writer, sheet_name='Horas_por_Periodo', index=False)
                    print("Tabela 'Horas por Período' salva com sucesso.")
            
            print(f"Relatório simplificado salvo em: {arquivo_excel}")
            return arquivo_excel
        
        # Salvar em Excel com abas por classificação
        print(f"\nCriando arquivo Excel com abas por classificação: {arquivo_excel}")
        with pd.ExcelWriter(arquivo_excel, engine='openpyxl') as writer:
            # Salvar cada conjunto de tabelas em abas separadas por classificação
            for classificacao, tabelas in tabelas_por_classificacao.items():
                # Limitar o nome da classificação para evitar problemas com nomes de abas
                classificacao_abreviada = str(classificacao)[:15].replace('/', '_').replace('\\', '_')
                
                # Salvar a tabela de contagem pivotada
                if 'contagem' in tabelas and tabelas['contagem'] is not None:
                    nome_aba = f"{classificacao_abreviada}_Contagem"
                    tabelas['contagem'].to_excel(writer, sheet_name=nome_aba, index=False)
                    print(f"Tabela de contagem para '{classificacao}' salva na aba '{nome_aba}'.")
                
                # Salvar a tabela de horas pivotada
                if 'horas' in tabelas and tabelas['horas'] is not None:
                    nome_aba = f"{classificacao_abreviada}_Horas"
                    tabelas['horas'].to_excel(writer, sheet_name=nome_aba, index=False)
                    print(f"Tabela de horas para '{classificacao}' salva na aba '{nome_aba}'.")
        
        print(f"\nRelatório por classificação salvo em: {arquivo_excel}")
        return arquivo_excel
        
    except Exception as e:
        print(f"ERRO ao salvar Excel: {e}")
        import traceback
        traceback.print_exc()
        
        # Método alternativo: salvar em CSV para cada classificação
        try:
            print("Tentando salvar tabelas em CSVs separados...")
            
            for classificacao, tabelas in tabelas_por_classificacao.items():
                classificacao_abreviada = str(classificacao)[:15].replace('/', '_').replace('\\', '_')
                
                if 'contagem' in tabelas and tabelas['contagem'] is not None:
                    arquivo_contagem = os.path.join(pasta_saida, f'{classificacao_abreviada}_contagem_{timestamp}.csv')
                    tabelas['contagem'].to_csv(arquivo_contagem, index=False)
                    print(f"Tabela de contagem para '{classificacao}' salva em: {arquivo_contagem}")
                
                if 'horas' in tabelas and tabelas['horas'] is not None:
                    arquivo_horas = os.path.join(pasta_saida, f'{classificacao_abreviada}_horas_{timestamp}.csv')
                    tabelas['horas'].to_csv(arquivo_horas, index=False)
                    print(f"Tabela de horas para '{classificacao}' salva em: {arquivo_horas}")
            
            return "Arquivos CSV separados por classificação salvos com sucesso."
            
        except Exception as e2:
            print(f"ERRO no método alternativo: {e2}")
            return None

        
"""
Correção final para os problemas de formatação de ano (2,023)
Esta função deve substituir a versão anterior do remover_todos_timezones
"""

def remover_todos_timezones(df):
    """
    Remove completamente todos os timezones de todas as colunas do DataFrame.
    Tratamento específico para o formato '2023-03-09 04:40:17 -03'.
    Corrige o problema de formatação do ano (2,023).
    
    Args:
        df (pandas.DataFrame): O DataFrame original
        
    Returns:
        pandas.DataFrame: DataFrame limpo sem nenhum timezone
    """
    # Criar uma cópia para não modificar o original
    df_limpo = df.copy()
    
    # Primeiro, converter qualquer coluna datetime com timezone para objeto (string)
    print("Convertendo todas as colunas datetime com timezone para string...")
    for coluna in df_limpo.columns:
        if pd.api.types.is_datetime64_dtype(df_limpo[coluna]):
            if hasattr(df_limpo[coluna].dt, 'tz') and df_limpo[coluna].dt.tz is not None:
                print(f"  - Convertendo {coluna} para string")
                df_limpo[coluna] = df_limpo[coluna].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Para colunas que são do tipo objeto (string), verificar se contêm datas com timezone
    # e tratá-las especificamente
    print("Verificando colunas de texto para formatos de data com timezone...")
    colunas_processadas = []
    
    for coluna in df_limpo.select_dtypes(include=['object']).columns:
        # Amostra para verificar se parece com data
        amostra = df_limpo[coluna].dropna().head(5).tolist()
        parece_data = False
        
        if amostra:
            for valor in amostra:
                if isinstance(valor, str) and ('-03' in valor or '+00' in valor or 'GMT' in valor):
                    parece_data = True
                    break
        
        if parece_data:
            print(f"  - Processando {coluna} com formato específico de data")
            # Tentar remover o timezone mantendo apenas a data e hora
            try:
                # Para o formato específico '2023-03-09 04:40:17 -03'
                def limpar_data(texto):
                    if not isinstance(texto, str):
                        return texto
                    
                    # Remover a parte do timezone
                    if ' -03' in texto:
                        return texto.split(' -03')[0]
                    elif ' +00' in texto:
                        return texto.split(' +00')[0]
                    elif ' GMT' in texto:
                        return texto.split(' GMT')[0]
                    return texto
                
                df_limpo[coluna] = df_limpo[coluna].apply(limpar_data)
                colunas_processadas.append(coluna)
            except Exception as e:
                print(f"    Erro ao processar {coluna}: {e}")
    
    # Verificar DataExecucao novamente, caso ainda seja um problema
    if 'DataExecucao' in df_limpo.columns:
        if pd.api.types.is_datetime64_dtype(df_limpo['DataExecucao']):
            print("Tratamento especial para DataExecucao")
            df_limpo['DataExecucao'] = df_limpo['DataExecucao'].astype(str)
    
    # Verificar DataCriacao, caso exista
    if 'DataCriacao' in df_limpo.columns:
        print("Tratamento especial para DataCriacao")
        if not pd.api.types.is_datetime64_dtype(df_limpo['DataCriacao']):
            # Se for string, extrair Ano e Mês
            try:
                # Primeiro verifica se tem o formato específico "2023-03-09 04:40:17 -03"
                amostra = df_limpo['DataCriacao'].iloc[0] if not df_limpo['DataCriacao'].empty else ""
                if isinstance(amostra, str) and '-03' in amostra:
                    print(f"  - Formato detectado: '2023-03-09 04:40:17 -03'")
                    # Extrai apenas a parte da data, ignorando o timezone
                    df_limpo['DataCriacao'] = df_limpo['DataCriacao'].apply(
                        lambda x: str(x).split(' -03')[0] if isinstance(x, str) and ' -03' in x else x
                    )
                    
                # Agora tenta converter para datetime
                df_limpo['DataCriacao'] = pd.to_datetime(df_limpo['DataCriacao'], errors='coerce')
            except Exception as e:
                print(f"  - Erro ao processar DataCriacao: {e}")
    
    # Adicionar colunas Ano e Mês garantindo que sejam inteiros
    if 'DataCriacao' in df_limpo.columns:
        try:
            if pd.api.types.is_datetime64_dtype(df_limpo['DataCriacao']):
                # Extrair ano e mês da coluna datetime e garantir que sejam inteiros
                df_limpo['Ano'] = df_limpo['DataCriacao'].dt.year.astype('int32')  # Forçar como inteiro
                df_limpo['Mes'] = df_limpo['DataCriacao'].dt.month.astype('int32')  # Forçar como inteiro
                print(f"  - Ano e Mês extraídos de DataCriacao. Amostra Ano: {df_limpo['Ano'].head(3).tolist()}")
            else:
                # Se ainda for string, fazer extração baseada em texto
                def extrair_ano(texto):
                    if isinstance(texto, str) and len(texto) >= 4:
                        try:
                            return int(texto[:4])  # Garantir que é inteiro
                        except:
                            return 2023  # Valor padrão
                    return 2023  # Valor padrão
                
                def extrair_mes(texto):
                    if isinstance(texto, str) and len(texto) >= 7 and texto[4] == '-':
                        try:
                            return int(texto[5:7])  # Garantir que é inteiro
                        except:
                            return 1  # Valor padrão
                    return 1  # Valor padrão
                
                df_limpo['Ano'] = df_limpo['DataCriacao'].apply(extrair_ano)
                df_limpo['Mes'] = df_limpo['DataCriacao'].apply(extrair_mes)
                print(f"  - Ano e Mês extraídos de texto. Amostra Ano: {df_limpo['Ano'].head(3).tolist()}")
        except Exception as e:
            print(f"Erro ao extrair Ano e Mês: {e}")
            df_limpo['Ano'] = 2023  # Valor padrão como inteiro
            df_limpo['Mes'] = 1  # Valor padrão como inteiro
    
    # Garantir que Ano e Mês sejam inteiros se já existirem no DataFrame
    if 'Ano' in df_limpo.columns:
        try:
            # Converter para int32 para evitar formatação com vírgula
            df_limpo['Ano'] = pd.to_numeric(df_limpo['Ano'], errors='coerce').fillna(2023).astype('int32')
            print(f"  - Coluna Ano convertida para inteiro. Amostra: {df_limpo['Ano'].head(3).tolist()}")
        except Exception as e:
            print(f"  - Erro ao converter Ano para inteiro: {e}")
    
    if 'Mes' in df_limpo.columns:
        try:
            # Converter para int32 para garantir que seja inteiro
            df_limpo['Mes'] = pd.to_numeric(df_limpo['Mes'], errors='coerce').fillna(1).astype('int32')
        except Exception as e:
            print(f"  - Erro ao converter Mes para inteiro: {e}")
    
    print(f"Processamento concluído. Colunas tratadas: {colunas_processadas}")
    return df_limpo      


def criar_tabelas_por_cluster(df):
    """
    Cria tabelas separadas para cada classificação (Cardiologia, Imagem, etc.)
    usando especificamente a coluna 'Classificacao' criada pelo processo de classificação.
    
    Args:
        df (pandas.DataFrame): DataFrame com os dados completos
        
    Returns:
        dict: Dicionário com DataFrames para cada classificação
    """
    # Inicializar o dicionário de tabelas
    tabelas_por_classificacao = {}
    
    # Verificar se df é None
    if df is None or df.empty:
        print("AVISO: DataFrame vazio ou None fornecido para criar_tabelas_por_cluster.")
        return tabelas_por_classificacao
    
    # Limpar e remover timezones
    df_limpo = remover_todos_timezones(df)
    
    # Verificar se existe a coluna 'Classificacao'
    if df_limpo is None or 'Classificacao' not in df_limpo.columns:
        print("AVISO: Coluna 'Classificacao' não encontrada. Criando classificação...")
        try:
            # Tentar classificar os dados
            from src.data_processing import classificar_vendas
            df_limpo = classificar_vendas(df_limpo)
            
            if df_limpo is None or 'Classificacao' not in df_limpo.columns:
                raise Exception("Classificação falhou")
        except Exception as e:
            print(f"ERRO ao classificar dados: {e}")
            print("Criando uma classificação fictícia 'Todos' para demonstração.")
            if df_limpo is None:
                import pandas as pd
                df_limpo = pd.DataFrame()
            df_limpo['Classificacao'] = 'Todos'
    
    # IMPORTANTE: Adicionar chamada para preparar_dados para criar a coluna 'hora'
    from src.data_processing import preparar_dados
    print("Preparando dados para adicionar coluna 'hora'...")
    df_limpo = preparar_dados(df_limpo)
    
    # Identificar todas as classificações únicas
    classificacoes = sorted(df_limpo['Classificacao'].dropna().unique())
    
    print(f"Classificações identificadas: {classificacoes}")
    
    # Criar tabelas separadas para cada classificação
    tabelas_por_classificacao = {}
    
    for classificacao in classificacoes:
        print(f"\nProcessando classificação: {classificacao}")
        
        # Filtrar dados pela classificação
        df_classificado = df_limpo[df_limpo['Classificacao'] == classificacao].copy()
        
        if df_classificado.empty:
            print(f"AVISO: Não há dados para a classificação '{classificacao}'. Ignorando.")
            continue
        
        print(f"Registros para a classificação '{classificacao}': {len(df_classificado)}")
        
        # Criar tabelas para esta classificação
        try:
            # Tabela de contagem
            tabela_unidades = criar_tabela_unidade_por_mes(df_classificado)
            tabela_unidades_pivot = formatar_tabela_pivot(tabela_unidades, 'Contagem')
            
            # Tabela de horas
            tabela_horas = criar_tabela_horas_por_mes(df_classificado)
            tabela_horas_pivot = formatar_tabela_pivot(tabela_horas, 'Total_Horas')
            
            # Armazenar no dicionário
            tabelas_por_classificacao[classificacao] = {
                'contagem': tabela_unidades_pivot,
                'horas': tabela_horas_pivot,
                'contagem_detalhada': tabela_unidades,
                'horas_detalhadas': tabela_horas
            }
            
            print(f"Tabelas criadas para a classificação '{classificacao}' com sucesso.")
        except Exception as e:
            print(f"ERRO ao criar tabelas para a classificação '{classificacao}': {e}")
            import traceback
            traceback.print_exc()
    
    return tabelas_por_classificacao


def formatar_tabela_pivot(df_tabela, valor_col='Contagem'):
    """
    Reformata uma tabela para ter unidades como linhas e períodos como colunas.
    
    Args:
        df_tabela (pandas.DataFrame): DataFrame com as colunas 'Centro', 'Ano', 'Mes', 'Periodo' e uma coluna de valor
        valor_col (str): Nome da coluna que contém os valores (ex: 'Contagem' ou 'Total_Horas')
        
    Returns:
        pandas.DataFrame: DataFrame pivotado com unidades nas linhas e períodos nas colunas
    """
    # Verificar se df_tabela é None
    if df_tabela is None:
        print("ERRO: A tabela fornecida é None.")
        return None
    
    # Verificar se as colunas necessárias existem
    colunas_necessarias = ['Centro', 'Periodo', valor_col]
    for col in colunas_necessarias:
        if col not in df_tabela.columns:
            print(f"ERRO: Coluna '{col}' não encontrada na tabela.")
            return df_tabela  # Retorna a tabela original se faltarem colunas
    
    # Converter para formato pivotado usando pandas pivot_table
    try:
        print(f"Reformatando tabela para ter períodos como colunas...")
        tabela_pivot = df_tabela.pivot_table(
            index='Centro',
            columns='Periodo',
            values=valor_col,
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # Verificar se há colunas além de 'Centro'
        if len(tabela_pivot.columns) <= 1:
            print("AVISO: Não há períodos para formatar como colunas.")
            return df_tabela
        
        # Ordenar as colunas de período cronologicamente
        colunas_periodo = [col for col in tabela_pivot.columns if col != 'Centro']
        colunas_periodo.sort()  # Ordem alfanumérica ordenará 2023-01, 2023-02, etc.
        
        # Reorganizar as colunas: primeiro Centro, depois períodos ordenados
        colunas_ordenadas = ['Centro'] + colunas_periodo
        tabela_pivot = tabela_pivot[colunas_ordenadas]
        
        print(f"Tabela reformatada com sucesso: {len(tabela_pivot)} linhas, {len(colunas_periodo)} períodos.")
        return tabela_pivot
    
    except Exception as e:
        print(f"ERRO ao reformatar tabela: {e}")
        import traceback
        traceback.print_exc()
        return df_tabela  # Retorna a tabela original em caso de erro



