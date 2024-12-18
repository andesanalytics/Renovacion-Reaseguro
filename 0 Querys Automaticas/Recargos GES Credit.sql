select * from
(select t.scerfol folio, t.polnum poliza, t2.polnum poliza_t0057, t.aserut rut, t2.aserut rut_t0057, t2.asesec secuencial,p.procod producto,c.ncobcod codigo_cobertura,
t2.polasefin fecha_efecto, t2.polasefsa fecha_vencimiento,
c.NCOBSOBTASDEP sobreprima_deporte, c.NCOBSOBTASACT sobreprima_actividad, c.NCOBSOBTASMED sobreprima_medico, c.NCobExtPri extraprima,
c.NCobSobMesAct meses_sobreprima_actividad,c.NCobSobMesMed meses_sobreprima_medico,c.NCobSobMesDep meses_sobreprima_deporte, c.NCobExtPriMes meses_extraprima, t.dpsultest estado_dps, 'NT0029C' origen
from altavida.t0029 t
left join altavida.t0057 t2 on t.scerfol=t2.polscerfol
, altavida.NT0029C c, altavida.polizas p
where t.scerfol=c.nscerfol
and t.polnum=c.npolnum
and p.polnum=t.polnum
and t.dpsultest in ('EX','AX')
and (c.NCOBSOBTASDEP + c.NCOBSOBTASACT + c.NCOBSOBTASMED>0 or c.NCobExtPri>0))
union all
(select t.scerfol folio, t.polnum poliza, t2.polnum poliza_t0057, t.aserut rut, t2.aserut rut_t0057, t2.asesec secuencial,p.procod producto,c.cobcod codigo_cobertura,
t2.polasefin fecha_efecto, t2.polasefsa fecha_vencimiento,
c.ScerSobreDep sobreprima_deporte, c.ScerSobreActiv sobreprima_actividad, c.ScerSobreMedic sobreprima_medico, c.ScerSobreEPrima extraprima,
c.ScerMesSActiv meses_sobreprima_actividad, c.ScerMesSMedic meses_sobreprima_medico,c.ScerMesSDep meses_sobreprima_deporte, c.ScerMesSEPrima meses_extraprima, t.dpsultest estado_dps, 'T0030' origen
from altavida.t0029 t
left join altavida.t0057 t2 on t.scerfol=t2.polscerfol
, altavida.t0030 c, altavida.polizas p
where t.scerfol=c.scerfol
and p.polnum=t.polnum
and t.dpsultest in ('EX','AX')
and (c.ScerSobreMedic + c.ScerSobreActiv + c.ScerSobreDep>0 or c.ScerSobreEPrima>0))