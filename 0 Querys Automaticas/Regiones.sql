select
        s.sseguro,s.npoliza poliza,s.ncertif certificado, s.sproduc producto,s.CACTIVI COD_PLAN,
        s.fefecto FECHA_EFECTO,s.fvencim fecha_vencimiento,
        s.fcaranu ffin_renov_anual,s.fanulac FECHA_ANULACION,
        d.cprovin region,	r.CCIUDAD ciudad,	d.CPOBLAC comuna,	(select translate(replace(d.tdomici,';',','),chr(10)||chr(11)||chr(13),' ') from dual) direccion
from
	seguros s,sitriesgo r,(select * from tomadores where nordtom=1) t  
    left join (select dir.sperson, dir.cprovin, dir.tdomici,dir.CPOBLAC,row_number() over (partition by dir.sperson order by dir.cprovin) as rank from per_direcciones dir ) d on t.sperson=d.sperson and d.rank = 1
where
        s.sseguro = r.sseguro
    and r.sseguro=t.sseguro
    and s.ccompani   = 2
    and s.creteni    = 0
    and (r.cprovin<1 or r.cprovin>16)