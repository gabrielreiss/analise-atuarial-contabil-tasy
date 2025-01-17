SELECT
T2.DT_MES_COMPETENCIA,
T5.CD_CONTA_DEB,
T3.NR_SEQUENCIA PLANO,
T5.VL_PROCEDIMENTO Valor,

T1.*, 
T2.*, 
--T3.*, 
--T4.*, 
T5.*--, 
--T6.*, 
--T7.*, 
--T8.*

FROM Tasy.PLS_CONTA T1

LEFT JOIN Tasy.PLS_PROTOCOLO_CONTA T2   ON T1.NR_SEQ_PROTOCOLO  = T2.NR_SEQUENCIA
LEFT JOIN Tasy.PLS_PLANO T3             ON T1.NR_SEQ_PLANO      = T3.NR_SEQUENCIA
--LEFT JOIN Tasy.PLS_PRESTADOR T4         ON T2.NR_SEQ_PRESTADOR  = T4.NR_SEQUENCIA
JOIN Tasy.PLS_CONTA_PROC T5             ON T1.NR_SEQUENCIA      = T5.NR_SEQ_CONTA
--LEFT JOIN Tasy.PROCEDIMENTO T6          ON T5.CD_PROCEDIMENTO   = T6.CD_PROCEDIMENTO
--LEFT JOIN Tasy.ctb_balancete_v T7       ON T5.CD_CONTA_DEB      = T7.CD_CONTA_CONTABIL 
--LEFT JOIN Tasy.CONTA_CONTABIL T8        ON T5.CD_CONTA_DEB      = T8.CD_CONTA_CONTABIL

WHERE
(T2.DT_MES_COMPETENCIA  >= TRUNC(SYSDATE, 'YEAR') AND T2.DT_MES_COMPETENCIA < ADD_MONTHS(TRUNC(SYSDATE, 'YEAR'), 12))
--AND Tasy.CTB_OBTER_SE_MES_FECHADO(T7.NR_SEQ_MES_REF, 2) = 'F'
AND T3.NR_SEQUENCIA IN (44,42,3)

--GROUP BY
--T2.DT_MES_COMPETENCIA,
--T5.CD_CONTA_DEB,
--T3.NR_SEQUENCIA

ORDER BY T2.DT_MES_COMPETENCIA ASC
