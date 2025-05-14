DECLARE @DataInicio AS DATE = '2023-01-01'
DECLARE @DataFinal AS DATE = GETDATE();

-- Create a temporary table for CardioSections to avoid the LIKE operation in the main query
DECLARE @FamiliaSections TABLE (Id INT PRIMARY KEY);
INSERT INTO @FamiliaSections
SELECT Id 
FROM GV_FamiliaProduto 
WHERE	Descricao LIKE '%Retorno%'
	 OR Descricao LIKE '%Consulta%';

-- Main query with optimized filters
SELECT DISTINCT
    COALESCE(cs.Nome, 'Não tem solicitante!') AS Solicitante,
    COALESCE(cats.Descricao, 'Não tem solicitante!') AS [Cat. Solicitante],    
    COALESCE(ce.Nome, 'Não tem executante!') AS Executante,
    COALESCE(cate.Descricao, 'Não tem executante!') AS [Cat. Executante],    
    e.sigla AS Centro,
    cdv.Documento + ' ' + cdv.Serie + '/' + CAST(cdv.Numero AS NVARCHAR) AS Documento,
    CONVERT(NVARCHAR, cdv.Data, 105) AS [Dt. Documento],
    CAST(cdv.DataCriacao AS SMALLDATETIME) AS DataCriacao,
    cdv.NumeroCliente,
    cdv.NomeCliente,
    prt.Descricao AS Protocolo,
    COALESCE(a_cdv.Numero, ldv.NumeroAnimal) AS IdAnimal,
    COALESCE(a_cdv.Nome, a_ldv.Nome) AS [Nome Animal],
    p.Codigo AS [Cod. Produto],
    p.Descricao AS Produto,
    fp.Descricao AS [Família],
    sfp.Descricao AS [Sub-família],
    scp.Descricao AS [Secção],
    ldv.Quantidade AS Quantidade,
    'Desconsiderar' AS 'Armazem',
    ldv.PV AS PreçoVenda,
    ldv.ValorTotal AS ValorVenda,
    CASE 
        WHEN p.Descricao LIKE '%Clube%' THEN pc.Pvp4
        ELSE pc.Pvp1 
    END AS [PVP1*],
    ldv.SubTotalDescontos AS [DescontoR$],
    CASE 
        WHEN p.Descricao LIKE '%Clube%' THEN (pc.Pvp4 * ldv.Quantidade)
        ELSE (pc.Pvp1 * ldv.Quantidade) 
    END AS ValorTotal,
    lcv.DataCriacao AS DataExecucao
FROM GV_CabecalhoDocumentoVenda cdv
INNER JOIN GV_LinhaDocumentoVenda ldv ON cdv.Id = ldv.IdCabecalhoDocumentoVenda AND ldv.TipoLinha = 'P'
LEFT JOIN GV_Cliente cli ON cli.Numero = cdv.NumeroCliente
LEFT JOIN GV_Colaborador cs ON cs.Id = ldv.IdColaboradorSolicitante
LEFT JOIN GV_Colaborador ce ON ce.Id = ldv.IdColaboradorExecutante
LEFT JOIN GV_Empresa e ON cdv.IdEmpresa = e.Id
LEFT JOIN GV_ProdutoCentro pc ON pc.NumeroProduto = ldv.NumeroProduto AND pc.IdCentro = cdv.IdCentro
LEFT JOIN GV_Produto p ON p.Numero = pc.NumeroProduto
LEFT JOIN GV_FamiliaProduto fp ON fp.Id = p.IdFamilia
LEFT JOIN GV_SeccaoProduto scp ON scp.Id = p.IdSeccao
LEFT JOIN GV_SubFamiliaProduto sfp ON sfp.Id = p.IdSubFamilia
LEFT JOIN GV_CategoriaProfissional cats ON cats.Id = cs.IdCategoria
LEFT JOIN GV_CategoriaProfissional cate ON cate.Id = ce.IdCategoria
LEFT JOIN GV_ProtocoloCliente prt ON prt.id = cli.Idprotocolo
LEFT JOIN GV_LinhaCarrinhoVendas lcv ON ldv.idlinhacarrinhovendas = lcv.id
LEFT JOIN GV_Animal a_cdv ON a_cdv.Numero = cdv.NumeroAnimal
LEFT JOIN GV_Animal a_ldv ON a_ldv.Numero = ldv.NumeroAnimal
WHERE cdv.Documento = 'FAT'
  AND cdv.Estado <> 'A'
  AND cdv.Data >= @DataInicio 
  AND cdv.Data <= @DataFinal
  AND p.IdFamilia IN (SELECT Id FROM @FamiliaSections)
--ORDER BY cdv.DataCriacao ASC;
