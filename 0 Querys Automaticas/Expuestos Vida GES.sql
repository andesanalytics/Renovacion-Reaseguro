select  
t.aserut rut,c.asesxocod sexo,c.asefecnac fec_nac,p.ctrrut rut_contratante,1 ccompani,0 sseguro,t.polnum poliza, t.asesec certificado, p.procod producto, 1 nriesgo, t.polasecco cod_cob, 
(Select PROPLNNOM  from ALTAVIDA.planesxproductos x where x.procod = p.procod and x.proplncod = p.polplncod) PLAN_DESC,
p.canvecod CANAL_VENTA,(Select CANVEDES from ALTAVIDA.CANVENTA x where x.canvecod = p.canvecod) CANAL_DESC, 
t.polcobope nro_operacion,
(Select count(1) from altavida.indaseadi  where indsolfol = p.Polsolfol and INDADIRUT <> p.CtrIndRut)+1 nro_riesgos, 
p.poltip tipo_poliza, t.polasevin fecha_efecto, t.polasevfi fecha_vencimiento,
polasecfi icapital, t.polasepri iprianu, p.polperioc forma_pago, p.polest estado,e.estnom DESC_ESTADO
from altavida.t0058 t,altavida.polizas p,altavida.clientes c,altavida.t0049 e
where t.polnum = p.polnum  
and t.aserut = c.aserut
and p.polest=e.estcod
and p.polest not in (98)
and t.polvigente not in (9)
and t.polasevin <= to_date('fecha_fin','yyyy-mm-dd')
and t.polasevfi >= to_date('fecha_inicio','yyyy-mm-dd')
and t.polasevfi > t.polasevin
and p.procod in (322,323,342,142,207,209,272,161,200,270,271,277,278,288,289,290,291,326,328,329,353,335)
