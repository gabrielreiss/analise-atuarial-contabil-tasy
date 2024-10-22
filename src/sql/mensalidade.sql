SELECT 
TO_CHAR(DT_MESANO_REFERENCIA, 'MM/YYYY'),
SUM(VL_LOTE)

FROM Tasy.PLS_LOTE_MENSALIDADE

WHERE 
DT_MESANO_REFERENCIA >= TRUNC(SYSDATE, 'YEAR') AND DT_MESANO_REFERENCIA < ADD_MONTHS(TRUNC(SYSDATE, 'YEAR'), 12)

GROUP BY TO_CHAR(DT_MESANO_REFERENCIA, 'MM/YYYY')
ORDER BY TO_CHAR(DT_MESANO_REFERENCIA, 'MM/YYYY')