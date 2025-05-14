DECLARE @DataInicio AS DATE = '2024-01-01'
DECLARE @DataFinal AS DATE = GETDATE();

SELECT 
    Hosp.NumeroAnimal,
    Hosp.DataEntrada,
    Hosp.DataSaida,
    Emp.Sigla AS Unidade,
    Zn.Descricao AS Zona,
    CASE 
        WHEN Zn.Descricao LIKE '%intensiva%' 
             AND Emp.Sigla IN ('BL', 'BT', 'CB', 'MA') THEN 'UTI' 
        ELSE 'Inter'
    END AS TipoZona
FROM 
    GV_Hospitalizacao Hosp
LEFT JOIN 
    GV_Empresa Emp ON Emp.Id = Hosp.IdCentro
LEFT JOIN 
    GV_Zona Zn ON Zn.Id = Hosp.IdZona
ORDER BY
    Hosp.DataEntrada DESC