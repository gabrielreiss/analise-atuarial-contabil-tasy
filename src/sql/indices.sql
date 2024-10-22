SELECT
    T1.DT_REFERENCIA competencia,
    
    (SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 12 THEN T1.VL_SALDO ELSE 0 END) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 21 THEN T1.VL_SALDO ELSE 1 END)) AS LC,

    (SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 121 THEN T1.VL_SALDO ELSE 0 END) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 21 THEN T1.VL_SALDO ELSE 1 END)) AS LC_imediata,

    (SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 46 THEN T1.VL_SALDO ELSE 0 END) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 31 THEN T1.VL_SALDO ELSE 1 END)) AS DA,

    ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 3 THEN T1.VL_SALDO ELSE 0 END) - 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 4 THEN T1.VL_SALDO ELSE 0 END)) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 31 THEN T1.VL_SALDO ELSE 1 END)) AS MLL,

    ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 3 THEN T1.VL_SALDO ELSE 0 END) - 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 4 THEN T1.VL_SALDO ELSE 0 END)) / 
     ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 25 THEN T1.VL_SALDO ELSE 1 END) + 
      SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 3  THEN T1.VL_SALDO ELSE 1 END) - 
      SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 4  THEN T1.VL_SALDO ELSE 1 END)))) AS ROE,

    ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 41 THEN T1.VL_SALDO ELSE 0 END) 
    - SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 415 THEN T1.VL_SALDO ELSE 0 END)
    - SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 416 THEN T1.VL_SALDO ELSE 0 END)
   ) / SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 31 THEN T1.VL_SALDO ELSE 1 END)) AS Sinistralidade,

     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 411 THEN T1.VL_SALDO ELSE 0 END)
    /SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 311 THEN T1.VL_SALDO ELSE 1 END) AS Sinistralidade_Bruta,

    (SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 43 THEN T1.VL_SALDO ELSE 0 END) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 31 THEN T1.VL_SALDO ELSE 1 END)) AS DC,

    ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 41 THEN T1.VL_SALDO ELSE 0 END) 
    - SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 415 THEN T1.VL_SALDO ELSE 0 END)
    - SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 416 THEN T1.VL_SALDO ELSE 0 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 43 THEN T1.VL_SALDO ELSE 0 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 46 THEN T1.VL_SALDO ELSE 0 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 441 THEN T1.VL_SALDO ELSE 0 END)
     ) / (
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 31 THEN T1.VL_SALDO ELSE 1 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 331 THEN T1.VL_SALDO ELSE 1 END)
     )) AS ICombinado,

    ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 41 THEN T1.VL_SALDO ELSE 0 END) 
    - SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 415 THEN T1.VL_SALDO ELSE 0 END)
    - SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 416 THEN T1.VL_SALDO ELSE 0 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 43 THEN T1.VL_SALDO ELSE 0 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 46 THEN T1.VL_SALDO ELSE 0 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 441 THEN T1.VL_SALDO ELSE 0 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 45 THEN T1.VL_SALDO ELSE 0 END)
     ) / (
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 31 THEN T1.VL_SALDO ELSE 1 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 331 THEN T1.VL_SALDO ELSE 1 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 35 THEN T1.VL_SALDO ELSE 1 END)
     )) AS ICombinado_ampl,

    ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 35 THEN T1.VL_SALDO ELSE 0 END) - 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 45 THEN T1.VL_SALDO ELSE 0 END)) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 31 THEN T1.VL_SALDO ELSE 1 END)) AS IRF,

    (SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 12 THEN T1.VL_SALDO ELSE 0 END)
    + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 13 THEN T1.VL_SALDO ELSE 0 END)
    ) / (SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 21 THEN T1.VL_SALDO ELSE 1 END)
        + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 23 THEN T1.VL_SALDO ELSE 1 END)
    ) AS LC_geral,

    (SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 133 THEN T1.VL_SALDO ELSE 0 END) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 1 THEN T1.VL_SALDO ELSE 1 END)) AS Imob_ativo,

    (SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 133 THEN T1.VL_SALDO ELSE 0 END) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 25 THEN T1.VL_SALDO ELSE 1 END)) AS Imob_PL,

    ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 21 THEN T1.VL_SALDO ELSE 0 END) + 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 23 THEN T1.VL_SALDO ELSE 0 END)) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 25 THEN T1.VL_SALDO ELSE 1 END)) AS CT_CP,

    ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 123 THEN T1.VL_SALDO ELSE 0 END)) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 31 THEN T1.VL_SALDO ELSE 1 END) * 360) AS PMCR,

    ((SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 21111102 THEN T1.VL_SALDO ELSE 0 END)
     + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 21111103 THEN T1.VL_SALDO ELSE 0 END)
     + SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 21112103 THEN T1.VL_SALDO ELSE 0 END)
     ) / 
     SUM(CASE WHEN T1.CD_CLASSIF_SEM_PONTO = 41 THEN T1.VL_SALDO ELSE 1 END) * 360) AS PMPE

FROM    Tasy.ctb_balancete_v T1

WHERE 
    ((T1.IE_NORMAL_ENCERRAMENTO = 'N' AND EXTRACT(MONTH FROM T1.DT_REFERENCIA) != 12)
    OR (EXTRACT(MONTH FROM T1.DT_REFERENCIA) = 12 AND T1.IE_NORMAL_ENCERRAMENTO = 'E')) 
    AND T1.DT_REFERENCIA BETWEEN 
        ADD_MONTHS(TRUNC(SYSDATE, 'MM'), - 61) + 1
        AND 
        TRUNC(SYSDATE, 'MM') - 1
    AND T1.CD_ESTABELECIMENTO = 2
    AND Tasy.CTB_OBTER_SE_MES_FECHADO(T1.NR_SEQ_MES_REF, 2) = 'F'
        
GROUP BY T1.DT_REFERENCIA
ORDER BY T1.DT_REFERENCIA DESC
