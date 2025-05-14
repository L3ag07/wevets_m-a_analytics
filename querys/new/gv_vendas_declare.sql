DECLARE @DataInicio AS DATE = '2023-01-01'
DECLARE @DataFinal AS DATE = GETDATE();

-- Tabelas temporárias para filtros de todas as consultas
-- Filtros para Seções de Produto
DECLARE @Sections TABLE (Id INT PRIMARY KEY);
INSERT INTO @Sections
SELECT Id 
FROM GV_SeccaoProduto 
WHERE Descricao LIKE '%Anestesia%'
   OR Descricao LIKE '%Cardiologia%'
   OR Descricao LIKE '%Imagem%';

-- Filtros para Famílias de Produto
DECLARE @FamiliaSections TABLE (Id INT PRIMARY KEY);
INSERT INTO @FamiliaSections
SELECT Id 
FROM GV_FamiliaProduto 
WHERE Descricao LIKE '%Cirurgia%'
   OR Descricao LIKE '%Retorno%'
   OR Descricao LIKE '%Consulta%';

-- Consulta principal com filtros otimizados
SELECT DISTINCT
    e.sigla AS Centro,
    CONVERT(NVARCHAR, cdv.Data, 105) AS DtDocumento,
    CAST(cdv.DataCriacao AS SMALLDATETIME) AS DataCriacao,
    cdv.NumeroCliente AS IdCliente,
    cli.CEP AS CepCliente,
    cli.BairroMorada AS BairroCliente,
    COALESCE(a_cdv.Numero, ldv.NumeroAnimal) AS IdAnimal, 
    scp.Descricao AS Secao,
    fp.Descricao AS Familia,
    sfp.Descricao AS SubFamilia,
    p.Codigo AS CodProduto,
    p.Descricao AS Produto,
    ldv.Quantidade AS Quantidade,
    ldv.PV AS PrecoVenda,
    ldv.ValorTotal AS ValorVenda,
    CASE 
        WHEN p.Descricao LIKE '%Clube%' THEN pc.Pvp4
        ELSE pc.Pvp1 
    END AS PVP1,
    ldv.SubTotalDescontos AS DescontoRS,
    CASE 
        WHEN p.Descricao LIKE '%Clube%' THEN (pc.Pvp4 * ldv.Quantidade)
        ELSE (pc.Pvp1 * ldv.Quantidade) 
    END AS ValorTotal,
    lcv.DataCriacao AS DataExecucao
FROM GV_CabecalhoDocumentoVenda cdv
INNER JOIN GV_LinhaDocumentoVenda ldv ON cdv.Id = ldv.IdCabecalhoDocumentoVenda AND ldv.TipoLinha = 'P'
LEFT JOIN GV_Empresa e ON cdv.IdEmpresa = e.Id
LEFT JOIN GV_ProdutoCentro pc ON pc.NumeroProduto = ldv.NumeroProduto AND pc.IdCentro = cdv.IdCentro
LEFT JOIN GV_Produto p ON p.Numero = pc.NumeroProduto
LEFT JOIN GV_FamiliaProduto fp ON fp.Id = p.IdFamilia
LEFT JOIN GV_SeccaoProduto scp ON scp.Id = p.IdSeccao
LEFT JOIN GV_SubFamiliaProduto sfp ON sfp.Id = p.IdSubFamilia
LEFT JOIN GV_Cliente cli ON cli.Numero = cdv.NumeroCliente
LEFT JOIN GV_LinhaCarrinhoVendas lcv ON ldv.idlinhacarrinhovendas = lcv.id
LEFT JOIN GV_Animal a_cdv ON a_cdv.Numero = cdv.NumeroAnimal
LEFT JOIN GV_Animal a_ldv ON a_ldv.Numero = ldv.NumeroAnimal
WHERE cdv.Documento = 'FAT'
  AND cdv.Estado <> 'A'
  AND cdv.Data >= @DataInicio 
  AND cdv.Data <= @DataFinal
  AND (
       p.IdSeccao IN (SELECT Id FROM @Sections)
    OR p.IdFamilia IN (SELECT Id FROM @FamiliaSections)
    )