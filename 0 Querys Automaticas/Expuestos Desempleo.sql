select
        per.NNUMIDE RUT,Decode(per.CSEXPER, 1, 'M', 2, 'F', ' ') sexo,per.FNACIMI fec_nac,
        s.ccompani,s.sseguro,s.npoliza poliza,s.ncertif certificado, s.sproduc producto,g.cgarant cod_cob,s.CACTIVI COD_PLAN,p.codoperacion nro_operacion,
        Nvl(Pp1.CVALPAR, 1) TIPO_POLIZA,
        s.fefecto FECHA_EFECTO,s.fvencim fecha_vencimiento,s.fanulac FECHA_ANULACION,
        (select mov.femisio from movseguro mov where s.sseguro=mov.sseguro and nmovimi=(select max(nmovimi) from movseguro where sseguro=mov.sseguro and cmovseg in (3) )) fecha_contabilizacion_anulacion,
        g.ICAPITAL,g.iprianu,s.CFORPAG FORMA_PAGO,s.csituac ESTADO,
        (select tatribu from detvalores where cvalor=61 and cidioma=3 and catribu=s.csituac) DESC_ESTADO,
        cred.tinteres tasa_cred,cred.periodicidad period_tasa,rec.ctiprec tipo_recibo
from
	seguros s
    left join (select distinct sseguro,tinteres,ncuotas,periodicidad from prestcuadroseg) cred on s.sseguro=cred.sseguro
    left join parproductos pp1 on s.sproduc = pp1.sproduc and pp1.CPARPRO = 'TIPO_POLIZA'
    left join (select distinct sseguro, ctiprec from recibos where ctiprec=9)  rec on rec.sseguro = s.sseguro
    ,garanseg  g,
	per_personas per,
    riesgos r,
    prestcuadroseg p
where
    s.sseguro = g.sseguro
    and g.ffinefe is null
    and r.sseguro = s.sseguro
	and r.sperson = per.sperson
    and s.sseguro=p.sseguro
    and p.nmovimi=1
    and s.ccompani   = 2
    and r.nriesgo = 1
    and s.creteni    = 0
    and r.sseguro=s.sseguro
    and r.nriesgo=g.nriesgo
    and s.sproduc in (10006)
    and g.cgarant in (9400,7800,7810,7820)
    
    