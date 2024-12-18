select 
t.aserut rut_contratante,2 ccompani,0 sseguro,t.polnum poliza, t.asesec certificado, p.procod producto, t.polasecco cod_cob, 
(Select PROPLNCOD  from ALTAVIDA.planesxproductos x where x.procod = p.procod and x.proplncod = p.polplncod) PLAN_DESC,t.polcobope nro_operacion,
p.poltip tipo_poliza, t.polasevin fecha_efecto, t.polasevfi fecha_vencimiento,
polasecfi icapital,
t.polcobtnet iprianu, 
p.polperioc forma_pago, p.polest estado,e.estnom DESC_ESTADO,
t2.polasebienreg region, t2.polasebienciud ciudad, t2.polasebiencom comuna, 
(select translate(replace(t2.polasebiendir,';',','),chr(10)||chr(11)||chr(13),' ') from dual) direccion
from general.t0058 t, general.polizas p,altavida.clientes c,altavida.t0049 e,general.t0057 t2
where t.polnum = p.polnum  
    and t.aserut = c.aserut
    and p.polest=e.estcod
    and t.polnum = t2.polnum 
    and t.aserut = t2.aserut
    and t.asesec = t2.asesec
    and p.polest not in (98)
    and t.polvigente not in (9)
    and t.polasevin <= to_date('fecha_fin','yyyy-mm-dd')
    and t.polasevfi >= to_date('fecha_inicio','yyyy-mm-dd')
    and t.polasevfi > t.polasevin
