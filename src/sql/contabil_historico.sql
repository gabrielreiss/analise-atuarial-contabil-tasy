SELECT
TO_CHAR(T1.DT_MOVIMENTO, 'YYYY-MM') AS COMPETENCIA,
T1.CD_HISTORICO,
T2.DS_HISTORICO,
--T1.DS_COMPL_HISTORICO,
COUNT(*) AS QNT,
SUM(CASE WHEN CD_CONTA_DEBITO = 44065 THEN VL_MOVIMENTO ELSE 0 END) - SUM(CASE WHEN CD_CONTA_CREDITO = 44065 THEN (VL_MOVIMENTO) ELSE 0 END) AS D44,
SUM(CASE WHEN CD_CONTA_DEBITO = 44064 THEN VL_MOVIMENTO ELSE 0 END) - SUM(CASE WHEN CD_CONTA_CREDITO = 44064 THEN (VL_MOVIMENTO) ELSE 0 END) AS Titulo_associativo,
SUM(CASE WHEN CD_CONTA_DEBITO IN (40019) THEN VL_MOVIMENTO ELSE 0 END) - SUM(CASE WHEN CD_CONTA_CREDITO IN (40019) THEN (VL_MOVIMENTO) ELSE 0 END) AS CA_medico,
SUM(CASE WHEN CD_CONTA_DEBITO IN (40072) THEN VL_MOVIMENTO ELSE 0 END) - SUM(CASE WHEN CD_CONTA_CREDITO IN (40072) THEN (VL_MOVIMENTO) ELSE 0 END) AS CA_odonto,
SUM(CASE WHEN CD_CONTA_DEBITO IN (40019, 40072) THEN VL_MOVIMENTO ELSE 0 END) - SUM(CASE WHEN CD_CONTA_CREDITO IN (40019, 40072) THEN (VL_MOVIMENTO) ELSE 0 END) AS CA


FROM tasy.CTB_MOVIMENTO T1

JOIN Tasy.HISTORICO_PADRAO T2 ON T1.CD_HISTORICO = T2.CD_HISTORICO

WHERE
(T1.CD_CONTA_DEBITO IN (44065, 44064, 40019, 40072) OR T1.CD_CONTA_CREDITO IN (44065, 44064, 40019, 40072))
AND T1.DT_MOVIMENTO >= TO_DATE('01-01-2024', 'DD-MM-YYYY')
--AND NR_SEQ_MES_REF >= 1565


GROUP BY 
TO_CHAR(DT_MOVIMENTO, 'YYYY-MM'),
T1.CD_HISTORICO,
T2.DS_HISTORICO
--,T1.DS_COMPL_HISTORICO

ORDER BY 
TO_CHAR(DT_MOVIMENTO, 'YYYY-MM'),
COUNT(*) DESC