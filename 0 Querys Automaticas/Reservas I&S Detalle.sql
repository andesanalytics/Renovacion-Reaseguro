select *
from general.RvaDet
where RVAPROCANO = año_proceso
and RVAPROCMES = mes_proceso
and RVARRCCED >0 
and cobcod in (120)
and POLNUM=5000000326
