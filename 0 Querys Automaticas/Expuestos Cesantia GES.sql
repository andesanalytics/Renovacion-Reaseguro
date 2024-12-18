select  
t.aserut rut,c.asesxocod sexo,c.asefecnac fec_nac,2 ccompani,0 sseguro,t.polnum poliza, t.asesec certificado, p.procod producto, t.polasecco cod_cob, 
(Select PROPLNCOD  from ALTAVIDA.planesxproductos x where x.procod = p.procod and x.proplncod = p.polplncod) PLAN_DESC,t.polcobope nro_operacion,
p.poltip tipo_poliza, 
t.polasevin fecha_efecto, t.polasevfi fecha_vencimiento,
d.DESGFPRE fecha_prepago,d.DESGFREN fecha_renuncia,t.polasevfi f_fin_vigencia,d.desgproceso periodo_contabilizacion,
polasecfi icapital, t.polcobtnet iprianu, p.polperioc forma_pago, p.polest estado,e.estnom DESC_ESTADO,d.DESGPRPAG, t3.POLASECREDTASINT TASA_CRED,t3.polasecredtasper period_tasa
from general.t0058 t
left join 
(select * from GENERAL.DESGDEVOEXCEL 
where desgpoliza in (5000000226,5000000227,5000000229,5000000230,5000000231,5000000269,5000000270,5000000272,5000000273,5000000274,5000000280,5000000287,5000000319,5000000328,5000000329,5000000331)
and DESGESTREG = 'GE' and DESGPRPAG>0) d 
on t.aserut=d.desgrut and t.asesec=d.desgasesec and t.polnum=d.desgpoliza
left join 
(select aserut,asesec,polaseope, POLASECREDTASINT, polperiodcre,polasecredtasper from altavida.t0057 where POLASECREDTASINT is not null and POLASECREDTASINT >0) t3 
on t.aserut = t3.aserut and t.asesec = t3.asesec and t.polcobope=t3.polaseope
,general.polizas p,altavida.clientes c,altavida.t0049 e
where t.polnum = p.polnum  
and t.aserut = c.aserut
and p.polest=e.estcod
and p.polest not in (98)
and t.polvigente not in (9)
and t.polnum in (5000000226,5000000227,5000000229,5000000230,5000000231,5000000269,5000000270,5000000272,5000000273,5000000274,5000000280,5000000287,5000000319,5000000328,5000000329,5000000331)