SELECT
    T1.DT_REFERENCIA competencia,
    T1.CD_CONTA_CONTABIL CD_CONTA_CONTABIL,
    T1.DS_CONTA_CONTABIL DS_CONTA_CONTABIL,
    T1.DS_GRUPO_CONTA DS_GRUPO_CONTA,
    T1.CD_CLASSIF_SEM_PONTO CD_CLASSIF_SEM_PONTO,
    SUM(T1.VL_DEBITO) VL_DEBITO,
    SUM(T1.VL_CREDITO) VL_CREDITO,
    SUM(T1.VL_SALDO) VL_SALDO,
    SUM(T1.VL_MOVIMENTO) VL_MOVIMENTO
    
FROM    Tasy.ctb_balancete_v T1

WHERE 
    ((T1.IE_NORMAL_ENCERRAMENTO = 'N' AND EXTRACT(MONTH FROM T1.DT_REFERENCIA) != 12)
    OR (EXTRACT(MONTH FROM T1.DT_REFERENCIA) = 12 AND T1.IE_NORMAL_ENCERRAMENTO = 'E')) 
    AND T1.DT_REFERENCIA >= TRUNC(SYSDATE, 'YEAR') AND T1.DT_REFERENCIA < ADD_MONTHS(TRUNC(SYSDATE, 'YEAR'), 12)
    AND T1.CD_ESTABELECIMENTO = 2
    AND Tasy.CTB_OBTER_SE_MES_FECHADO(T1.NR_SEQ_MES_REF, 2) = 'F'
    AND CD_CONTA_CONTABIL IN (31832, 30014, 31394, 30052, 32946, 31424, 31937, 32947, 31420, 31421, 31953, 33198)

GROUP BY 
T1.DT_REFERENCIA,
T1.CD_CONTA_CONTABIL,
T1.DS_CONTA_CONTABIL,
T1.DS_GRUPO_CONTA,
T1.CD_CLASSIF_SEM_PONTO

ORDER BY 
T1.DT_REFERENCIA,
T1.CD_CLASSIF_SEM_PONTO