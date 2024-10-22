SELECT
    a.nr_sequencia AS nr_seq_guia_monitor,
    a.nr_seq_conta,
    a.cd_guia_prestador,
    a.cd_guia_operadora,
    a.CD_CPF_CGC_PREST_EXEC,
    a.nr_seq_lote_monitor AS nr_seq_lote_envio,
    b.ds_lote,
    TO_CHAR(b.dt_mes_competencia, 'MM/YYYY') AS dt_mes_comp,
    DECODE(b.ie_tipo_lote, 0, 'Envio', 1, 'Exclusão', 2, 'Alteração') AS ds_tipo_envio,
    b.dt_geracao_lote,
    a.nr_seq_prestador,
    CASE
        WHEN a.nr_seq_prestador IS NULL
        THEN SUBSTR(tasy.pls_obter_dados_prest_inter(a.nr_seq_prest_inter, 'N'), 1, 255)
        ELSE SUBSTR(tasy.pls_obter_dados_prestador(a.nr_seq_prestador, 'N'), 1, 255)
    END AS nm_prestador,
    SUBSTR(tasy.pls_obter_dados_segurado(nr_seq_segurado, 'N'), 1, 255) AS nm_beneficiario,
    nr_seq_segurado,
    vl_cobranca_guia,
    COALESCE(TRUNC(a.dt_conta_fechada, 'DD'), TRUNC(a.dt_pagamento_previsto, 'DD'), TRUNC(a.dt_conta_fechada_recurso, 'DD'), TRUNC(a.dt_pagamento_recurso, 'DD')) AS dt_evento,
    pa.nm_arquivo AS nm_arquivo_envio,
    a.cd_cnes_prest_exec,
    (
        SELECT
            MAX(x.ie_tipo_evento)
        FROM
            tasy.pls_monitor_tiss_cta_val x
        INNER JOIN
            tasy.pls_monitor_tiss_alt_guia z ON z.nr_seq_cta_val = x.nr_sequencia
        WHERE
            z.nr_seq_guia_monitor = a.nr_sequencia
    ) AS ie_tipo_evento,
    (
        SELECT
            MAX(x.nr_sequencia)
        FROM
            tasy.pls_monitor_tiss_lote_com x
        INNER JOIN
            tasy.pls_monitor_tiss_lote_ret z ON x.nr_sequencia = z.nr_seq_lote_com
        WHERE
            z.nr_sequencia = COALESCE(c.nr_seq_lote_monitor_ret, d.nr_seq_lote_monitor_ret)
    ) AS nr_seq_lote_com,
    COALESCE(c.nr_seq_lote_monitor_ret, d.nr_seq_lote_monitor_ret) AS nr_seq_lote_monitor_ret,
    
    a.*, b.*, c.*, d.*, pa.*

FROM
          tasy.pls_monitor_tiss_guia     a
JOIN      tasy.pls_monitor_tiss_lote     b ON a.nr_seq_lote_monitor = b.nr_sequencia
LEFT JOIN tasy.pls_monitor_tiss_guia_ret c ON a.nr_sequencia        = c.nr_seq_guia_monitor
LEFT JOIN tasy.pls_mon_tiss_guia_ret_act d ON a.nr_sequencia        = d.nr_seq_guia_monitor
LEFT JOIN tasy.pls_monitor_tiss_arquivo pa ON pa.nr_sequencia       = a.nr_seq_arq_monitor

WHERE
    a.CD_GUIA_OPERADORA IN ('5008474')

