select  
t.aserut rut,c.asesxocod sexo,c.asefecnac fec_nac,p.ctrrut rut_contratante,1 ccompani,0 sseguro,t.polnum poliza, t.asesec certificado, p.procod producto, 1 nriesgo, t.polasecco cod_cob, 
(Select PROPLNNOM  from ALTAVIDA.planesxproductos x where x.procod = p.procod and x.proplncod = p.polplncod) PLAN_DESC,t.polcobope nro_operacion,
(Select count(1) from altavida.indaseadi  where indsolfol = p.Polsolfol and INDADIRUT <> p.CtrIndRut)+1 nro_riesgos, 
p.poltip tipo_poliza, 
t.polasevin fecha_efecto, case when EXTRACT(YEAR FROM t.polfecvigfc)<1990 then t.polasevfi else t.polfecvigfc end fecha_vencimiento,
d.DESGFPRE fecha_prepago,d.DESGFREN fecha_renuncia,t.polasevfi fin_vigencia,d.desgproceso periodo_contabilizacion,
polasecfi icapital, t.polcobtnet iprianu, p.polperioc forma_pago, p.polest estado,e.estnom DESC_ESTADO
from altavida.t0058 t
left join 
(select * from ALTAVIDA.DESGDEVOEXCEL 
where desgpoliza in (565,568) and DESGESTREG = 'GE' and DESGPRPAG>0) d 
on t.aserut=d.desgrut and t.asesec=d.desgasesec and t.polnum=d.desgpoliza
, altavida.polizas p,altavida.clientes c,altavida.t0049 e
where t.polnum = p.polnum  
and t.aserut = c.aserut
and p.polest=e.estcod
and p.polest not in (98)
and t.polvigente not in (9)
and t.polasevfi >= t.polasevin
and t.polnum in (565,568)