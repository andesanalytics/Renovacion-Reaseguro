select  
t.aserut rut,c.asesxocod sexo,c.asefecnac fec_nac,p.ctrrut rut_contratante,1 ccompani,0 sseguro,t.polnum poliza, t.asesec certificado, p.procod producto, 1 nriesgo, t.polasecco cod_cob, 
(Select PROPLNNOM  from ALTAVIDA.planesxproductos x where x.procod = p.procod and x.proplncod = p.polplncod) PLAN_DESC,t.polcobope nro_operacion,
p.poltip tipo_poliza, t.polasevin fecha_efecto, t.polasevfi fecha_vencimiento,
t.polfecvigic f_inicio_cred,t.polfecvigfc f_fin_cred,
polasecfi  ,polcfiori,
t.polasepri iprianu,
p.polperioc forma_pago, p.polest estado,e.estnom DESC_ESTADO
,t2.polasecredtasint tasa_cred, t2.polasecredtasper period_tasa
from altavida.t0058 t,altavida.t0057 t2,altavida.polizas p,altavida.clientes c,altavida.t0049 e
where t.polnum = p.polnum  
and t.polnum = t2.polnum 
and t.aserut = t2.aserut
and t.asesec = t2.asesec
and t.aserut = c.aserut
and p.polest=e.estcod
and p.polest not in (98)
and t.polvigente not in (9)
and t.polasevin <= to_date('fecha_fin','yyyy-mm-dd')
and t.polasevfi >= to_date('fecha_inicio','yyyy-mm-dd')
and t.polasevfi > t.polasevin
and p.procod not in (322,323,342,270,271,277,278,288,289,290,291,326,328,329,353,335,354,88,101,193) 
and t.polnum not in (478, 479, 523, 524, 525, 526, 557, 558, 559, 563, 565, 568, 567)