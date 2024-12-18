select sdoinsrut rut, sdoinsnumope nro_operacion,sdoinspolnum poliza, sdoinssalins saldo_insoluto
from altavida.movsdoins 
where sdoinsper in ('periodo_fin')
and sdoinssalins>0
