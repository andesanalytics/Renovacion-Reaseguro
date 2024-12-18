select s.INDPOLNUM poliza_solicitud, p.polnum poliza, s.subaserut rut, 1 secuencial, s.procod producto,i.cobcodind codigo_cobertura,
i.cobvalrec valor_recargo,i.conrecporc porcentaje_recargo
from altavida.RECINDIVIDUAL i, altavida.solicitud s, altavida.INDEVA ind, altavida.polizas p
where 
ind.indestceva in ('AX','EX')
and i.indsolfol=s.indsolfol
and s.indsolfol=ind.indsolfol
and s.indsolfol=p.POLSOLFOL