SELECT DISTINCT
    e.sigla AS Centro,
    CONVERT(NVARCHAR, cdv.Data, 105) AS DtDocumento,
    cdv.Documento + ' ' + cdv.Serie + '/' + cast(cdv.Numero AS NVARCHAR) AS Documento,
    CONVERT(VARCHAR(23), cdv.DataCriacao, 120) AS DataCriacao,  -- Convertido para string
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
    CONVERT(VARCHAR(23), lcv.DataCriacao, 120) AS DataExecucao  -- Convertido para string
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
  AND cdv.Data >= '2023-01-01' 
  AND cdv.Data <= GETDATE()
  AND (
       p.IdSeccao IN (
           SELECT Id 
           FROM GV_SeccaoProduto 
           WHERE Descricao LIKE '%Cardiologia%'
              OR Descricao LIKE '%Imagem%'
       )
       OR 
       p.IdFamilia IN (
           SELECT Id 
           FROM GV_FamiliaProduto 
           WHERE Descricao LIKE '%Cirurgia%'
              OR Descricao LIKE '%Retorno%'
              OR Descricao LIKE '%Consulta%'
       )
  )