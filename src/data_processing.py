"""
Módulo para processamento e classificação dos dados de vendas.
Removida a classificação "Outros" e simplificado o processo.
Adicionada a coluna "hora" com base nas regras de classificação.
"""

import pandas as pd

def classificar_vendas(df):
    """
    Classifica os registros de vendas conforme regras específicas.
    
    Args:
        df (pandas.DataFrame): DataFrame com os dados de vendas.
        
    Returns:
        pandas.DataFrame: DataFrame com os dados classificados.
    """
    if df is None or df.empty:
        print("ERRO: DataFrame vazio ou nulo para classificação.")
        return None
        
    print("Iniciando classificação dos dados...")
    
    # Verificando se a coluna Classificacao já existe para evitar processamentos repetidos
    if 'Classificacao' in df.columns:
        print("Coluna 'Classificacao' já existe no DataFrame. Pulando classificação.")
        
        # Mostrar distribuição das classificações existentes
        contagem = df['Classificacao'].value_counts()
        print("\nDistribuição das classificações existentes:")
        for classe, quantidade in contagem.items():
            print(f"- {classe}: {quantidade} registros")
            
        return df
    
    # Verificando se as colunas necessárias existem
    colunas_necessarias = ['Secao', 'Familia']
    for coluna in colunas_necessarias:
        if coluna not in df.columns:
            print(f"ERRO: Coluna '{coluna}' não encontrada no DataFrame.")
            print(f"Colunas disponíveis: {', '.join(df.columns)}")
            return None
    
    # Criando a coluna de classificação
    def determinar_classificacao(row):
        """
        Função para determinar a classificação de cada registro.
        
        Regras:
        - Se a seção for "Cardiologia" → classifica como "Cardiologia"
        - Se a seção for "Imagem" → classifica como "Imagem"
        - Se a seção for "Anestesia" ou a família for "Cirurgia" → classifica como "Bloco Cirurgico"
        - Se a família for "Retorno" ou "Consulta" → classifica como "Clinica"
        - Nota: A query SQL deve garantir que somente registros que se enquadram nestas condições sejam retornados
        """
        # Tratando valores NaN nas colunas
        secao = str(row['Secao']).strip() if pd.notna(row['Secao']) else ""
        familia = str(row['Familia']).strip() if pd.notna(row['Familia']) else ""
        
        # Aplicando regras de classificação
        if secao == 'Cardiologia':
            return 'Cardiologia'
        elif secao == 'Imagem':
            return 'Imagem'
        elif secao == 'Anestesia' or familia == 'Cirurgia':
            return 'Bloco Cirurgico'
        elif familia in ['Retorno', 'Consulta']:
            return 'Clinica'
        else:
            # Assume-se que a query SQL filtra apenas registros relevantes
            # Retornar uma classificação válida em vez de "Outros"
            return 'Clinica'  # Classificação padrão em caso de não correspondência
    
    # Aplicando a função de classificação a cada linha
    df['Classificacao'] = df.apply(determinar_classificacao, axis=1)
    
    # Contagem de registros por classificação para validação
    contagem = df['Classificacao'].value_counts()
    print("\nDistribuição das classificações:")
    for classe, quantidade in contagem.items():
        print(f"- {classe}: {quantidade} registros")
    
    print("Classificação concluída com sucesso!")
    return df

def preparar_dados(df):
    """
    Versão simplificada que prepara os dados mínimos necessários para análise,
    sem otimizações ou transformações complexas.
    Adiciona a coluna "hora" com base nas regras de classificação.
    
    Args:
        df (pandas.DataFrame): DataFrame com os dados de vendas.
        
    Returns:
        pandas.DataFrame: DataFrame processado para análise.
    """
    if df is None or df.empty:
        print("ERRO: DataFrame vazio ou nulo para preparação.")
        return None
        
    print("Iniciando preparação simplificada dos dados...")
    
    # Verificar se a coluna 'hora' já existe para evitar processamento repetido
    if 'hora' in df.columns:
        print("Coluna 'hora' já existe no DataFrame. Pulando preparação.")
        # Mostrar estatísticas da coluna hora
        print(f"Estatísticas da coluna 'hora':")
        print(f"- Valores únicos: {df['hora'].nunique()}")
        print(f"- Média: {df['hora'].mean():.2f} horas")
        return df
    
    # Fazendo uma cópia para evitar modificações no DataFrame original
    df_processado = df.copy()
    
    # Lidando com valores ausentes em colunas numéricas essenciais
    colunas_numericas = ['ValorVenda']
    for coluna in colunas_numericas:
        if coluna in df_processado.columns:
            # Substituindo valores ausentes por zero
            df_processado[coluna] = df_processado[coluna].fillna(0)
    
    # Adicionando colunas úteis para análise temporal
    if 'DataCriacao' in df_processado.columns:
        # Converter para datetime apenas se necessário
        if not pd.api.types.is_datetime64_dtype(df_processado['DataCriacao']):
            df_processado['DataCriacao'] = pd.to_datetime(df_processado['DataCriacao'], errors='coerce')
        
        # Adicionando colunas de data formatadas para agrupamentos
        df_processado['Ano'] = df_processado['DataCriacao'].dt.year
        df_processado['Mes'] = df_processado['DataCriacao'].dt.month
    
    # Verificando se a coluna Classificacao existe (deve ter sido criada pela função classificar_vendas)
    if 'Classificacao' not in df_processado.columns:
        print("AVISO: Coluna 'Classificacao' não encontrada. Executando classificação primeiro...")
        df_processado = classificar_vendas(df_processado)
        if df_processado is None:
            return None
    
    # Adicionando a coluna "hora" com base nas regras de classificação
    def determinar_hora(row):
        """
        Função para determinar a hora de cada registro com base na classificação.
        
        Regras:
        - Cardiologia: 1.5 horas se Familia for "Consultas" ou "Consulta", senão 0.75 horas
        - Clinica: 1 hora
        - Imagem: 0.5 hora quando o centro for "RB", senão 0.67 hora
        - Bloco Cirurgico: 3 horas
        """
        classificacao = row['Classificacao'] if 'Classificacao' in row else ""
        familia = str(row['Familia']).strip() if pd.notna(row['Familia']) else ""
        centro = str(row['Centro']).strip() if 'Centro' in row and pd.notna(row['Centro']) else ""
        
        if classificacao == 'Cardiologia':
            if familia in ['Consultas', 'Consulta']:
                return 1.5
            else:
                return 0.75
        elif classificacao == 'Clinica':
            return 1.0
        elif classificacao == 'Imagem':
            if centro == 'RB':
                return 0.5
            else:
                return 0.67
        elif classificacao == 'Bloco Cirurgico':
            return 3.0
        else:
            # Valor padrão caso nenhuma regra se aplique
            return 0.0
    
    # Verificando se a coluna Centro existe
    if 'Centro' not in df_processado.columns:
        print("AVISO: Coluna 'Centro' não encontrada. A regra para Imagem pode não funcionar corretamente.")
    
    # Aplicando a função de determinação de hora a cada linha
    print("Adicionando coluna 'hora' com base nas regras de classificação...")
    df_processado['hora'] = df_processado.apply(determinar_hora, axis=1)
    
    # Verificar se a coluna foi criada e mostrar estatísticas
    if 'hora' in df_processado.columns:
        print("Coluna 'hora' adicionada com sucesso!")
        print(f"Estatísticas da coluna 'hora':")
        print(f"- Total: {len(df_processado)} registros")
        print(f"- Valores únicos: {df_processado['hora'].nunique()}")
        print(f"- Média: {df_processado['hora'].mean():.2f} horas")
        
        # Mostrar valores por classificação
        for classe in df_processado['Classificacao'].unique():
            media = df_processado[df_processado['Classificacao'] == classe]['hora'].mean()
            print(f"- Média para {classe}: {media:.2f} horas")
    else:
        print("ERRO: Não foi possível criar a coluna 'hora'.")
    
    print("Preparação simplificada dos dados concluída!")
    return df_processado