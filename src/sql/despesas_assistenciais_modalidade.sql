SELECT 
TO_CHAR(T2.DT_MES_COMPETENCIA, 'MM/YYYY') Competencia,
T3.IE_TIPO_CONTRATACAO,
T3.IE_REGULAMENTACAO,
T3.IE_SEGMENTACAO,
SUM(T1.VL_COBRADO) Cobrado,
SUM(T1.VL_GLOSA) GLOSA,
SUM(T1.VL_COPARTICIPACAO) Coparticipacao,
SUM(T1.VL_TOTAL) TOTAL

--T1.*, T2.*, T3.*

FROM Tasy.PLS_CONTA T1

LEFT JOIN Tasy.PLS_PROTOCOLO_CONTA T2 ON T1.NR_SEQ_PROTOCOLO = T2.NR_SEQUENCIA
LEFT JOIN Tasy.PLS_PLANO T3 ON T1.NR_SEQ_PLANO = T3.NR_SEQUENCIA

WHERE
T2.DT_MES_COMPETENCIA  >= TRUNC(SYSDATE, 'YEAR') AND T2.DT_MES_COMPETENCIA < ADD_MONTHS(TRUNC(SYSDATE, 'YEAR'), 12)

GROUP BY 
TO_CHAR(T2.DT_MES_COMPETENCIA, 'MM/YYYY'),
T3.IE_TIPO_CONTRATACAO,
T3.IE_REGULAMENTACAO,
T3.IE_SEGMENTACAO

ORDER BY 1 ASC